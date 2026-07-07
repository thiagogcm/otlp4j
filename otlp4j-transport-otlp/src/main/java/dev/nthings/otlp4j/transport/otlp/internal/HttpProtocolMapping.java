package dev.nthings.otlp4j.transport.otlp.internal;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.server.HttpService;
import dev.nthings.otlp4j.codec.DeliveryResultStatuses;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.transport.common.AdmissionControl;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.zip.GZIPInputStream;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Builds the per-signal OTLP/HTTP [HttpService]s served by the Armeria server, and maps a decoded
/// request and its [DeliveryResult] onto an HTTP response. The mapping mirrors the legacy OTLP/HTTP
/// transport:
///
/// - POST-only (the route restricts the method, so a GET yields `405`).
/// - `application/x-protobuf` content type required; `application/json` and other types yield `415`,
///   an absent header is tolerated (OTLP/JSON is not supported).
/// - gzip request bodies are inflated, capped at the decoded-size limit (`413` on overflow); an
///   uncompressed body over the limit is `413`.
/// - malformed protobuf yields `400`.
/// - an admission rejection yields `429` with a `Retry-After` header when a throttle hint is set.
/// - a [DeliveryResult.Accepted] or [DeliveryResult.Partial] yields `200` with the protobuf response;
///   a retryable rejection yields `503` (with `Retry-After` when hinted), a permanent one `500`; a
///   dispatcher failure yields `500`.
final class HttpProtocolMapping {

    private static final Logger log = LoggerFactory.getLogger(HttpProtocolMapping.class);

    private static final MediaType PROTOBUF = MediaType.parse(OtlpHttpPaths.CONTENT_TYPE);

    private HttpProtocolMapping() {}

    /// The OTLP/HTTP service for `codec`, gated by the shared admission control and capped at
    /// `maxBytes` decoded request bytes.
    ///
    /// @param codec       the signal codec
    /// @param dispatchers the per-signal dispatch functions
    /// @param maxBytes    the decoded-request size cap
    /// @param admission   the shared admission control
    /// @return the HTTP service
    static HttpService service(
            SignalCodec<?, ?, ?> codec, Dispatchers dispatchers, int maxBytes, AdmissionControl admission) {
        return (ctx, req) -> HttpResponse.of(
                req.aggregate()
                        // Run the decode and dispatch on Armeria's blocking-task executor rather than
                        // the connection's I/O event loop, so a blocking SignalConsumer cannot stall the
                        // shared event loop and every other connection multiplexed onto it.
                        .thenComposeAsync(
                                agg -> handle(agg, codec, dispatchers, maxBytes, admission),
                                ctx.blockingTaskExecutor())
                        .exceptionally(HttpProtocolMapping::onError));
    }

    private static <T, REQ extends MessageLite, RESP extends MessageLite> CompletionStage<HttpResponse> handle(
            AggregatedHttpRequest agg,
            SignalCodec<T, REQ, RESP> codec,
            Dispatchers dispatchers,
            int maxBytes,
            AdmissionControl admission) {
        if (!isProtobuf(agg.contentType())) {
            var ct = agg.contentType();
            return completed(text(HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                    "unsupported Content-Type " + (ct == null ? "(none)" : "'" + ct + "'")
                            + "; this receiver accepts only application/x-protobuf (OTLP/JSON is not supported)"));
        }

        byte[] body;
        try {
            body = readBody(agg, maxBytes);
        } catch (PayloadTooLargeException e) {
            return completed(text(HttpStatus.REQUEST_ENTITY_TOO_LARGE, e.getMessage()));
        } catch (IOException e) {
            return completed(text(HttpStatus.BAD_REQUEST, "could not read request body: " + e.getMessage()));
        }

        // Global admission: the permit spans decode and dispatch, released when the dispatch settles.
        var permit = admission.tryAcquire(() -> body.length);
        if (permit == null) {
            log.warn("receiver at capacity ({} requests / {} bytes in flight); responding 429",
                    admission.inFlightRequests(), admission.inFlightBytes());
            return completed(atCapacity(admission.retryAfterHint()));
        }

        REQ request;
        try {
            request = codec.parseRequest(body);
        } catch (InvalidProtocolBufferException e) {
            permit.close();
            return completed(text(HttpStatus.BAD_REQUEST, "malformed protobuf: " + e.getMessage()));
        }

        return dispatchers.dispatch(codec.proto(request))
                .handle((result, error) -> mapResult(codec, result, error))
                .whenComplete((response, error) -> permit.close());
    }

    private static <T, REQ extends MessageLite, RESP extends MessageLite> HttpResponse mapResult(
            SignalCodec<T, REQ, RESP> codec, @Nullable DeliveryResult result, @Nullable Throwable error) {
        if (error != null) {
            var cause = unwrap(error);
            log.warn("OTLP/HTTP dispatcher failed; responding 500", cause);
            return text(HttpStatus.INTERNAL_SERVER_ERROR, cause.getMessage());
        }
        if (result instanceof DeliveryResult.Rejected rejected) {
            var status = HttpStatus.valueOf(DeliveryResultStatuses.httpStatus(rejected));
            log.warn("OTLP/HTTP dispatcher rejected the whole batch; responding {}: {}",
                    status.code(), rejected.message());
            var retryAfter = DeliveryResultStatuses.retryAfterSeconds(rejected);
            if (retryAfter.isPresent()) {
                return textWithRetryAfter(status, retryAfter.getAsLong(), rejected.message());
            }
            return text(status, rejected.message());
        }
        return HttpResponse.of(HttpStatus.OK, PROTOBUF, codec.deliveryResponse(result).toByteArray());
    }

    /// `429` with a `Retry-After` header when `hint` is set, otherwise a plain `429`.
    private static HttpResponse atCapacity(@Nullable Duration hint) {
        if (hint != null) {
            return textWithRetryAfter(HttpStatus.TOO_MANY_REQUESTS,
                    DeliveryResultStatuses.retryAfterSeconds(hint), "receiver at capacity");
        }
        return text(HttpStatus.TOO_MANY_REQUESTS, "receiver at capacity");
    }

    /// Whether `contentType` names the OTLP/HTTP binary-protobuf media type. A null header is
    /// tolerated (some clients omit it); otherwise only `application/x-protobuf` is accepted,
    /// case-insensitively, ignoring any parameters. Anything else, including `application/json`, is
    /// not protobuf.
    private static boolean isProtobuf(@Nullable MediaType contentType) {
        if (contentType == null) {
            return true;
        }
        return "application".equalsIgnoreCase(contentType.type())
                && "x-protobuf".equalsIgnoreCase(contentType.subtype());
    }

    /// Reads the request body, transparently inflating `Content-Encoding: gzip`, capped at `maxBytes`
    /// decoded.
    private static byte[] readBody(AggregatedHttpRequest agg, int maxBytes) throws IOException {
        var raw = agg.content().array();
        var encoding = agg.headers().get(HttpHeaderNames.CONTENT_ENCODING);
        if (encoding != null && encoding.toLowerCase(Locale.ROOT).contains("gzip")) {
            return inflate(raw, maxBytes);
        }
        if (raw.length > maxBytes) {
            throw new PayloadTooLargeException("request body exceeds " + maxBytes + " bytes");
        }
        return raw;
    }

    private static byte[] inflate(byte[] gzipped, int maxBytes) throws IOException {
        var out = new ByteArrayOutputStream(Math.min(maxBytes, 64 * 1024));
        var buffer = new byte[8192];
        var total = 0;
        try (InputStream in = new GZIPInputStream(new java.io.ByteArrayInputStream(gzipped))) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                total += read;
                if (total > maxBytes) {
                    throw new PayloadTooLargeException("request body exceeds " + maxBytes + " bytes");
                }
                out.write(buffer, 0, read);
            }
        }
        return out.toByteArray();
    }

    private static HttpResponse onError(Throwable error) {
        var cause = unwrap(error);
        log.warn("OTLP/HTTP request handling failed; responding 500", cause);
        return text(HttpStatus.INTERNAL_SERVER_ERROR, cause.getMessage());
    }

    private static Throwable unwrap(Throwable error) {
        return error.getCause() != null
                && (error instanceof java.util.concurrent.CompletionException
                        || error instanceof java.util.concurrent.ExecutionException)
                ? error.getCause()
                : error;
    }

    private static HttpResponse text(HttpStatus status, @Nullable String message) {
        return HttpResponse.of(status, MediaType.PLAIN_TEXT_UTF_8, message == null ? "" : message);
    }

    private static HttpResponse textWithRetryAfter(HttpStatus status, long seconds, @Nullable String message) {
        var headers = ResponseHeaders.builder(status)
                .contentType(MediaType.PLAIN_TEXT_UTF_8)
                .add(HttpHeaderNames.RETRY_AFTER, Long.toString(seconds))
                .build();
        return HttpResponse.of(headers, HttpData.ofUtf8(message == null ? "" : message));
    }

    private static CompletionStage<HttpResponse> completed(HttpResponse response) {
        return CompletableFuture.completedFuture(response);
    }

    /// Signals that a request body exceeded the decoded-size cap, mapped to `413`.
    private static final class PayloadTooLargeException extends IOException {
        PayloadTooLargeException(String message) {
            super(message);
        }
    }
}
