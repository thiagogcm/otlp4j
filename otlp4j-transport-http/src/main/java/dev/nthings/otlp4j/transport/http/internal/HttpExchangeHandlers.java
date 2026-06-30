package dev.nthings.otlp4j.transport.http.internal;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.codec.DeliveryResults;
import dev.nthings.otlp4j.codec.LogsMapper;
import dev.nthings.otlp4j.codec.MetricsMapper;
import dev.nthings.otlp4j.codec.ProfilesMapper;
import dev.nthings.otlp4j.codec.SignalResponses;
import dev.nthings.otlp4j.codec.TraceMapper;
import dev.nthings.otlp4j.spi.Dispatchers;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Registers the four OTLP/HTTP collector endpoints on an [HttpServer], decoding
/// binary-protobuf export requests and dispatching to per-signal handlers.
///
/// The response contract mirrors the gRPC adapters:
/// - [ConsumeResult.Accepted] / [ConsumeResult.Partial] -> `200` with the
///   protobuf response (partial success carries rejected count and message).
/// - [ConsumeResult.Rejected] without cause -> `503` (retryable); with cause -> `500` (permanent).
/// - Malformed body -> `400`, oversized -> `413`, non-POST -> `405`, dispatcher error -> `500`.
final class HttpExchangeHandlers {

    private static final Logger log = LoggerFactory.getLogger(HttpExchangeHandlers.class);

    private HttpExchangeHandlers() {
    }

    static void register(HttpServer server, Dispatchers dispatchers, int maxBytes) {
        server.createContext(OtlpHttp.TRACES_PATH, handler(
                ExportTraceServiceRequest::parseFrom, TraceMapper::toDomain,
                dispatchers.traces(), SignalResponses::traces, maxBytes));
        server.createContext(OtlpHttp.METRICS_PATH, handler(
                ExportMetricsServiceRequest::parseFrom, MetricsMapper::toDomain,
                dispatchers.metrics(), SignalResponses::metrics, maxBytes));
        server.createContext(OtlpHttp.LOGS_PATH, handler(
                ExportLogsServiceRequest::parseFrom, LogsMapper::toDomain,
                dispatchers.logs(), SignalResponses::logs, maxBytes));
        server.createContext(OtlpHttp.PROFILES_PATH, handler(
                ExportProfilesServiceRequest::parseFrom, ProfilesMapper::toDomain,
                dispatchers.profiles(), SignalResponses::profiles, maxBytes));
    }

    private static <REQ, SIG> HttpHandler handler(
            ProtoParser<REQ> parse,
            Function<REQ, SIG> toDomain,
            Function<SIG, CompletionStage<ConsumeResult<SIG>>> dispatcher,
            Function<ConsumeResult<SIG>, ? extends MessageLite> asResponse,
            int maxBytes) {
        return exchange -> handle(exchange, parse, toDomain, dispatcher, asResponse, maxBytes);
    }

    private static <REQ, SIG> void handle(
            HttpExchange exchange,
            ProtoParser<REQ> parse,
            Function<REQ, SIG> toDomain,
            Function<SIG, CompletionStage<ConsumeResult<SIG>>> dispatcher,
            Function<ConsumeResult<SIG>, ? extends MessageLite> asResponse,
            int maxBytes) throws IOException {
        try {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                respond(exchange, 405, "text/plain", "method not allowed".getBytes(StandardCharsets.UTF_8));
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

            REQ request;
            try {
                request = parse.parse(payload);
            } catch (InvalidProtocolBufferException e) {
                respondText(exchange, 400, "malformed protobuf: " + e.getMessage());
                return;
            }

            ConsumeResult<SIG> result;
            try {
                result = dispatcher.apply(toDomain.apply(request)).toCompletableFuture().join();
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

            if (result instanceof ConsumeResult.Rejected<SIG>(var message, var cause)) {
                var status = DeliveryResults.httpStatus((ConsumeResult.Rejected<?>) result);
                log.warn("OTLP/HTTP dispatcher rejected the whole batch; responding {}: {}", status, message);
                respondText(exchange, status, message);
                return;
            }
            respond(exchange, 200, OtlpHttp.CONTENT_TYPE, asResponse.apply(result).toByteArray());
        } finally {
            exchange.close();
        }
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
