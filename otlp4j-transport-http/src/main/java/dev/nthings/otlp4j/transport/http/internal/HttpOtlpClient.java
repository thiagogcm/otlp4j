package dev.nthings.otlp4j.transport.http.internal;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.codec.Transports;
import dev.nthings.otlp4j.transport.common.ClientHeaders;
import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.transport.common.internal.ResilienceRetries;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The OTLP/HTTP implementation of the [OtlpClient] SPI: POSTs binary-protobuf export requests to a
/// collector's `/v1/<signal>` endpoints over the JDK [HttpClient].
///
/// Honours the full [ClientConfig]: TLS, per-request headers, gzip, deadline, and retries.
/// Each export runs on a virtual-thread carrier so blocking send and backoff never tie up a
/// platform thread.
///
/// **Internal.** Part of the transport layer; obtained via the SPI.
public final class HttpOtlpClient implements OtlpClient {

    private static final Logger log = LoggerFactory.getLogger(HttpOtlpClient.class);

    /// Runs the blocking teardown on a virtual thread, greppable by thread name if it stalls.
    private static final Executor SHUTDOWN_EXECUTOR =
            task -> Thread.ofVirtual().name("otlp-http-client-shutdown").start(task);

    /// Statuses the OTLP/HTTP client may retry, per the OTLP spec (plus 408).
    private static final Set<Integer> RETRYABLE_STATUS = Set.of(408, 429, 502, 503, 504);

    private final ClientConfig config;
    private final HttpClient http;
    private final boolean compress;
    private final Map<SignalDescriptor<?>, HttpEndpoint<?, ?, ?>> endpoints = new LinkedHashMap<>();
    private final ExecutorService executor;
    private final Retry retry;

    public HttpOtlpClient(ClientConfig config) {
        this.config = config;
        this.compress = config.compression() == Compression.GZIP;

        var scheme = config.tls() instanceof Tls.Disabled ? "http" : "https";
        // config.path() is a normalized prefix.
        var base = scheme + "://" + authority(config.host(), config.port()) + config.path();
        // Per-signal endpoint binding: each entry pairs the resolved request URI with its
        // SignalCodec. The PROFILES path stays the /v1development oddball because it is read
        // straight from OtlpHttp.PROFILES_PATH, never templated from the signal name.
        endpoints.put(SignalDescriptor.TRACES, new HttpEndpoint<>(
                URI.create(base + OtlpHttp.TRACES_PATH), SignalCodec.TRACES));
        endpoints.put(SignalDescriptor.METRICS, new HttpEndpoint<>(
                URI.create(base + OtlpHttp.METRICS_PATH), SignalCodec.METRICS));
        endpoints.put(SignalDescriptor.LOGS, new HttpEndpoint<>(
                URI.create(base + OtlpHttp.LOGS_PATH), SignalCodec.LOGS));
        endpoints.put(SignalDescriptor.PROFILES, new HttpEndpoint<>(
                URI.create(base + OtlpHttp.PROFILES_PATH), SignalCodec.PROFILES));

        var explicitConnect = config.connectTimeout();
        var connectTimeout = explicitConnect != null ? explicitConnect : config.timeout();
        var builder = HttpClient.newBuilder().connectTimeout(connectTimeout);
        switch (config.tls()) {
            case Tls.Disabled _ -> { /* plaintext http */ }
            case Tls.SystemTrust _ -> { /* https with the JVM default SSLContext */ }
            case Tls.Custom(var certFile, var keyFile, var trustFile) -> {
                Transports.requireCompleteClientMutualTls(certFile, keyFile);
                builder.sslContext(PemSsl.clientContext(certFile, keyFile, trustFile));
            }
            case Tls.Inline(var cert, var key, var trust) -> {
                Transports.requireCompleteClientMutualTls(cert, key);
                builder.sslContext(PemSsl.clientContext(cert, key, trust));
            }
            // Unlike gRPC, the JDK client uses a caller-built SSLContext verbatim, mTLS included.
            case Tls.SslContext(var sslContext, var _) -> builder.sslContext(sslContext);
        }
        this.http = builder.build();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.retry = ResilienceRetries.create(
                "otlp-http",
                config.retry(),
                HttpOtlpClient::isRetryable,
                HttpOtlpClient::retryAfter);
        log.debug("created OTLP/HTTP client for {}", base);
    }

    /// Wraps an IPv6 literal in brackets to form a valid URL authority; other hosts pass through.
    private static String authority(String host, int port) {
        var h = host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
        return h + ":" + port;
    }

    @Override
    public CompletionStage<DeliveryResult> export(TelemetryPayload payload, Provenance provenance) {
        // endpoints is populated at construction with one entry per signal, keyed by descriptor
        // identity, so the payload's signal selects the endpoint whose codec parses its response.
        var endpoint = endpoints.get(payload.signal());
        // Double-deadline, belt-and-suspenders and intentional: the HttpRequest.timeout (in
        // request(...)) bounds each in-flight connection, while this orTimeout bounds the whole
        // async attempt (retry backoff and executor queueing included), so a stalled carrier
        // thread cannot outlive the caller's budget. A slow call is interrupted by whichever
        // fires first.
        return CompletableFuture.supplyAsync(() -> send(endpoint, payload, provenance), executor)
                .orTimeout(config.timeout().toNanos(), TimeUnit.NANOSECONDS);
    }

    /// Writes `payload` for `endpoint` and sends it through Resilience4j retry. The request body is
    /// serialised once, outside the retry, into a buffer presized from the payload's serialized size,
    /// and re-sent on each attempt; a 2xx response is parsed and mapped to [DeliveryResult] attributed
    /// to `provenance`, any other outcome throws, mirroring the gRPC client. For a pass-through
    /// proto-backed payload the write is a direct proto serialisation with no domain-to-proto
    /// re-encode.
    private <T, REQ extends MessageLite, RESP extends MessageLite> DeliveryResult send(
            HttpEndpoint<T, REQ, RESP> endpoint, TelemetryPayload payload, Provenance provenance) {
        var codec = endpoint.codec();
        var size = Math.toIntExact(payload.estimatedSerializedSize());
        var body = compress ? gzipDirect(payload, size) : writeDirect(payload, size);
        var httpRequest = request(endpoint.uri(), body);
        return ResilienceRetries.execute(retry,
                () -> sendOnce(endpoint.uri(), httpRequest, codec::parseResponse,
                        resp -> codec.deliveryResult(resp, provenance)));
    }

    /// Test accessor: the resolved request URI bound to `signal`.
    URI uri(SignalDescriptor<?> signal) {
        return endpoints.get(signal).uri();
    }

    /// Binds a resolved request URI to its [SignalCodec] for one signal. The codec carries the proto
    /// marshalling; the URI stays transport-local because it is HTTP-specific.
    private record HttpEndpoint<T, REQ extends MessageLite, RESP extends MessageLite>(
            URI uri, SignalCodec<T, REQ, RESP> codec) {
    }

    private <RESP> DeliveryResult sendOnce(
            URI uri, HttpRequest httpRequest, ProtoParser<RESP> parse, Function<RESP, DeliveryResult> toResult) {
        HttpResponse<byte[]> response;
        try {
            response = http.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException("OTLP/HTTP export to " + uri + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException("interrupted during OTLP/HTTP export to " + uri);
        }

        var code = response.statusCode();
        if (code >= 200 && code < 300) {
            return toResult.apply(parseResponse(parse, response.body()));
        }
        throw rejection(uri, response);
    }

    private HttpRequest request(URI uri, byte[] body) {
        var builder = HttpRequest.newBuilder(uri)
                .timeout(config.timeout())
                .header("Content-Type", OtlpHttp.CONTENT_TYPE);
        if (compress) {
            builder.header("Content-Encoding", "gzip");
        }
        ClientHeaders.resolve(config.headers(), config.headerSupplier()).forEach(builder::header);
        return builder.POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
    }

    private static <RESP> RESP parseResponse(ProtoParser<RESP> parse, byte[] body) {
        try {
            return parse.parse(body);
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException("malformed OTLP/HTTP response body", e);
        }
    }

    private static OtlpHttpException rejection(URI uri, HttpResponse<byte[]> response) {
        var code = response.statusCode();
        var body = response.body();
        var text = new String(body, StandardCharsets.UTF_8).strip();
        var message =
                "OTLP/HTTP export to " + uri + " rejected: HTTP " + code + (text.isEmpty() ? "" : " - " + text);
        return new OtlpHttpException(message, code, retryAfter(response));
    }

    private static boolean isRetryable(Throwable error) {
        return error instanceof UncheckedIOException
                || error instanceof OtlpHttpException e && RETRYABLE_STATUS.contains(e.statusCode());
    }

    private static Duration retryAfter(Throwable error) {
        return error instanceof OtlpHttpException e && e.retryAfter() != null
                ? e.retryAfter()
                : Duration.ZERO;
    }

    private static Duration retryAfter(HttpResponse<?> response) {
        return response.headers()
                .firstValue("Retry-After")
                .map(HttpOtlpClient::parseRetryAfter)
                .orElse(Duration.ZERO);
    }

    private static Duration parseRetryAfter(String value) {
        var text = value.strip();
        try {
            var delay = Duration.ofSeconds(Long.parseLong(text));
            return delay.isNegative() ? Duration.ZERO : delay;
        } catch (NumberFormatException _) {
            try {
                var delay = Duration.between(
                        Instant.now(), ZonedDateTime.parse(text, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant());
                return delay.isNegative() ? Duration.ZERO : delay;
            } catch (DateTimeParseException _) {
                return Duration.ZERO;
            }
        }
    }

    /// Serialises `payload` into a single `byte[size]` and returns that same array, with no second
    /// defensive copy. The backing array is sized to `estimatedSerializedSize()` up front, so the proto
    /// writer fills it exactly; unlike `ByteArrayOutputStream`, whose `toByteArray()` is specified to
    /// always return a fresh `Arrays.copyOf`, the fixed-buffer stream hands the filled array straight
    /// to the body publisher. So the uncompressed path materialises exactly one request-body buffer.
    ///
    /// The buffer is exactly sized, so this relies on `estimatedSerializedSize()` being exact, its
    /// documented contract. That coupling is verified rather than trusted: if `writeTo` emits fewer
    /// bytes than declared the array would otherwise be POSTed padded with trailing NUL bytes, and if it
    /// emits more the fixed buffer overruns. Both are turned into a loud [IllegalStateException] so a
    /// non-conforming payload fails the export instead of silently corrupting the wire message, and
    /// because it is not an [IOException] it is not misclassified as a retryable transport fault.
    private static byte[] writeDirect(TelemetryPayload payload, int size) {
        var body = new byte[size];
        var out = new FixedBufferOutputStream(body);
        try {
            payload.writeTo(out);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize OTLP/HTTP request body", e);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalStateException(
                    "TelemetryPayload wrote more than its estimatedSerializedSize() of " + size
                            + " bytes; estimatedSerializedSize() must be exact",
                    e);
        }
        if (out.written() != size) {
            throw new IllegalStateException("TelemetryPayload wrote " + out.written()
                    + " bytes but estimatedSerializedSize() reported " + size
                    + "; estimatedSerializedSize() must be exact");
        }
        return body;
    }

    /// Serialises `payload` straight into a `GZIPOutputStream`, using `size` only as a capacity hint
    /// for the backing buffer to avoid regrowth. The one copy inherent to compressed output (the
    /// `toByteArray()` at the end) is unavoidable because the compressed length is not known ahead of
    /// time; it is unchanged from the byte-array gzip helper this replaces.
    private static byte[] gzipDirect(TelemetryPayload payload, int size) {
        var out = new ByteArrayOutputStream(size);
        try (var gz = new GZIPOutputStream(out)) {
            payload.writeTo(gz);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to gzip OTLP/HTTP request body", e);
        }
        return out.toByteArray();
    }

    /// A minimal [java.io.OutputStream] over a fixed, presized backing array. It never grows and never
    /// copies, so a proto message serialised into it fills the array in place and that same array is
    /// handed to the body publisher. The array is sized to the message's serialized length up front,
    /// so the writes never overrun it.
    private static final class FixedBufferOutputStream extends java.io.OutputStream {

        private final byte[] buffer;
        private int position;

        FixedBufferOutputStream(byte[] buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int b) {
            buffer[position++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) {
            System.arraycopy(b, off, buffer, position, len);
            position += len;
        }

        /// Number of bytes written so far, used to verify the payload filled the presized buffer exactly.
        int written() {
            return position;
        }
    }

    /// Interrupt-responsive teardown on a named shutdown carrier, bounded by `timeout`: drains the
    /// per-export executor under the remaining budget (one deadline, clamped at zero via
    /// `Math.max(0, ...)` so the wall-clock cannot exceed `timeout`), then forcibly closes the JDK
    /// client. `forceFlush` is the inherited no-op (the client does not buffer).
    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        return CompletableFuture.runAsync(() -> {
            var deadline = System.nanoTime() + timeout.toNanos();
            drainExecutor(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
            http.shutdownNow();
        }, SHUTDOWN_EXECUTOR);
    }

    /// Drains the per-export executor within `budget`, interrupting outstanding exports on timeout.
    private void drainExecutor(Duration budget) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(budget.toNanos(), TimeUnit.NANOSECONDS)) {
                log.warn("per-export executor did not terminate within {}; interrupting outstanding exports",
                        budget);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
