package dev.nthings.otlp4j.transport.http.internal;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dev.nthings.otlp4j.transport.common.AdmissionControl;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.codec.DeliveryResultStatuses;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.spi.Dispatchers;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Registers the four OTLP/HTTP collector endpoints on an [HttpServer], decoding
/// binary-protobuf export requests and dispatching to per-signal handlers.
///
/// The response contract mirrors the gRPC adapters:
/// - [DeliveryResult.Accepted] / [DeliveryResult.Partial] -> `200` with the
///   protobuf response (partial success carries rejected count and message).
/// - A retryable [DeliveryResult.Rejected] -> `503` (with a `Retry-After` header when the
///   rejection carries a retry-after hint); a permanent one -> `500`.
/// - Malformed body -> `400`, oversized -> `413`, non-POST -> `405`, dispatcher error -> `500`.
/// - A `Content-Type` other than `application/x-protobuf` -> `415` (an absent header is
///   tolerated, since some clients omit it). This receiver is protobuf-only: OTLP/JSON is not
///   accepted. The `415` choice follows HTTP content negotiation convention (RFC 9110) and the
///   OpenTelemetry Collector's own behavior for an unsupported media type; the OTLP spec itself
///   does not mandate a status code for a Content-Type mismatch, only for malformed data (`400`).
final class HttpExchangeHandlers {

    private static final Logger log = LoggerFactory.getLogger(HttpExchangeHandlers.class);

    private HttpExchangeHandlers() {
    }

    /// Per-signal routing table: each entry pairs a [SignalCodec] with its endpoint path. Explicit
    /// data (not a name template) so the `/v1development/profiles` oddball is carried as a value.
    private record Route<T, REQ extends MessageLite, RESP extends MessageLite>(
            SignalCodec<T, REQ, RESP> codec, String path) {
    }

    private static final List<Route<?, ?, ?>> ROUTES = List.of(
            new Route<>(SignalCodec.TRACES, OtlpHttp.TRACES_PATH),
            new Route<>(SignalCodec.METRICS, OtlpHttp.METRICS_PATH),
            new Route<>(SignalCodec.LOGS, OtlpHttp.LOGS_PATH),
            new Route<>(SignalCodec.PROFILES, OtlpHttp.PROFILES_PATH));

    static void register(HttpServer server, Dispatchers dispatchers, int maxBytes, AdmissionControl admission) {
        for (var route : ROUTES) {
            server.createContext(route.path(), handler(route.codec(), dispatchers, maxBytes, admission));
        }
    }

    private static <T, REQ extends MessageLite, RESP extends MessageLite> HttpHandler handler(
            SignalCodec<T, REQ, RESP> codec,
            Dispatchers dispatchers,
            int maxBytes,
            AdmissionControl admission) {
        return exchange -> handle(exchange,
                codec::parseRequest,
                request -> dispatchers.dispatch(codec.proto(request)),
                codec::deliveryResponse, maxBytes, admission);
    }

    private static <REQ> void handle(
            HttpExchange exchange,
            ProtoParser<REQ> parse,
            Function<REQ, CompletionStage<DeliveryResult>> dispatcher,
            Function<DeliveryResult, ? extends MessageLite> asResponse,
            int maxBytes,
            AdmissionControl admission) throws IOException {
        try {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                respond(exchange, 405, "text/plain", "method not allowed".getBytes(StandardCharsets.UTF_8));
                return;
            }

            var contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if (!isProtobuf(contentType)) {
                respondText(exchange, 415, "unsupported Content-Type "
                        + (contentType == null ? "(none)" : "'" + contentType + "'")
                        + "; this receiver accepts only application/x-protobuf (OTLP/JSON is not supported)");
                return;
            }

            byte[] payload;
            try {
                payload = readBody(exchange, maxBytes);
            } catch (PayloadTooLargeException e) {
                respondText(exchange, 413, e.getMessage());
                return;
            } catch (IOException e) {
                respondText(exchange, 400, "could not read request body: " + e.getMessage());
                return;
            }

            // Global admission: the permit spans decode and dispatch; the handler is
            // synchronous (join below), so the finally releases it exactly once.
            var permit = admission.tryAcquire(() -> payload.length);
            if (permit == null) {
                var hint = admission.retryAfterHint();
                if (hint != null) {
                    exchange.getResponseHeaders().add("Retry-After",
                            Long.toString(DeliveryResultStatuses.retryAfterSeconds(hint)));
                }
                log.warn("receiver at capacity ({} requests / {} bytes in flight); responding 429",
                        admission.inFlightRequests(), admission.inFlightBytes());
                respondText(exchange, 429, "receiver at capacity");
                return;
            }
            try {
                dispatchAdmitted(exchange, parse, dispatcher, asResponse, payload);
            } finally {
                permit.close();
            }
        } finally {
            exchange.close();
        }
    }

    private static <REQ> void dispatchAdmitted(
            HttpExchange exchange,
            ProtoParser<REQ> parse,
            Function<REQ, CompletionStage<DeliveryResult>> dispatcher,
            Function<DeliveryResult, ? extends MessageLite> asResponse,
            byte[] payload) throws IOException {
        REQ request;
        try {
            request = parse.parse(payload);
        } catch (InvalidProtocolBufferException e) {
            respondText(exchange, 400, "malformed protobuf: " + e.getMessage());
            return;
        }

        DeliveryResult result;
        try {
            result = dispatcher.apply(request).toCompletableFuture().join();
        } catch (CompletionException e) {
            var cause = e.getCause() != null ? e.getCause() : e;
            log.warn("OTLP/HTTP dispatcher failed; responding 500", cause);
            respondText(exchange, 500, cause.getMessage());
            return;
        } catch (RuntimeException e) {
            log.warn("OTLP/HTTP decode or dispatch failed; responding 500", e);
            respondText(exchange, 500, e.getMessage());
            return;
        }

        if (result instanceof DeliveryResult.Rejected rejected) {
            var status = DeliveryResultStatuses.httpStatus(rejected);
            DeliveryResultStatuses.retryAfterSeconds(rejected).ifPresent(
                    seconds -> exchange.getResponseHeaders().add("Retry-After", Long.toString(seconds)));
            log.warn("OTLP/HTTP dispatcher rejected the whole batch; responding {}: {}",
                    status, rejected.message());
            respondText(exchange, status, rejected.message());
            return;
        }
        respond(exchange, 200, OtlpHttp.CONTENT_TYPE, asResponse.apply(result).toByteArray());
    }

    /// Whether `contentType` names the OTLP/HTTP binary-protobuf media type. A null header is
    /// tolerated (some clients omit it); otherwise only the media type before any `;` parameter
    /// (for example `; charset=utf-8`) is compared, case-insensitively, against
    /// [OtlpHttp#CONTENT_TYPE]. Anything else, including `application/json` and `text/plain`, is
    /// not protobuf.
    private static boolean isProtobuf(@Nullable String contentType) {
        if (contentType == null) {
            return true;
        }
        var semicolon = contentType.indexOf(';');
        var mediaType = (semicolon < 0 ? contentType : contentType.substring(0, semicolon)).strip();
        return mediaType.equalsIgnoreCase(OtlpHttp.CONTENT_TYPE);
    }

    /// Reads the request body, transparently inflating `Content-Encoding: gzip`,
    /// capped at `maxBytes` decoded.
    private static byte[] readBody(HttpExchange exchange, int maxBytes) throws IOException {
        var encoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
        InputStream in = exchange.getRequestBody();
        if (encoding != null && encoding.toLowerCase(Locale.ROOT).contains("gzip")) {
            in = new GZIPInputStream(in);
        }
        var out = new ByteArrayOutputStream(Math.min(maxBytes, 64 * 1024));
        var buffer = new byte[8192];
        var total = 0;
        int read;
        while ((read = in.read(buffer)) != -1) {
            total += read;
            if (total > maxBytes) {
                throw new PayloadTooLargeException("request body exceeds " + maxBytes + " bytes");
            }
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private static void respondText(HttpExchange exchange, int status, String message) throws IOException {
        respond(exchange, status, "text/plain; charset=utf-8",
                (message == null ? "" : message).getBytes(StandardCharsets.UTF_8));
    }

    private static void respond(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().add("Content-Type", contentType);
        if (body.length == 0) {
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        exchange.sendResponseHeaders(status, body.length);
        try (var os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    /// Signals that a request body exceeded the decoded-size cap, mapped to `413`.
    private static final class PayloadTooLargeException extends IOException {
        PayloadTooLargeException(String message) {
            super(message);
        }
    }
}
