package dev.nthings.otlp4j.transport.http.internal;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.codec.LogsMapper;
import dev.nthings.otlp4j.codec.MetricsMapper;
import dev.nthings.otlp4j.codec.ProfilesMapper;
import dev.nthings.otlp4j.codec.TraceMapper;
import dev.nthings.otlp4j.codec.Transports;
import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.config.Tls;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;
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

    private static final long CLOSE_TIMEOUT_SECONDS = 5;

    /// Statuses the OTLP/HTTP client may retry, per the OTLP spec (plus 408).
    private static final Set<Integer> RETRYABLE_STATUS = Set.of(408, 429, 502, 503, 504);

    private final ClientConfig config;
    private final HttpClient http;
    private final boolean compress;
    private final URI tracesUri;
    private final URI metricsUri;
    private final URI logsUri;
    private final URI profilesUri;
    private final ExecutorService executor;

    public HttpOtlpClient(ClientConfig config) {
        this.config = config;
        this.compress = config.compression() == Compression.GZIP;

        var scheme = config.tls() instanceof Tls.Disabled ? "http" : "https";
        // config.path() is a normalized prefix.
        var base = scheme + "://" + authority(config.host(), config.port()) + config.path();
        this.tracesUri = URI.create(base + OtlpHttp.TRACES_PATH);
        this.metricsUri = URI.create(base + OtlpHttp.METRICS_PATH);
        this.logsUri = URI.create(base + OtlpHttp.LOGS_PATH);
        this.profilesUri = URI.create(base + OtlpHttp.PROFILES_PATH);

        var builder = HttpClient.newBuilder().connectTimeout(config.timeout());
        switch (config.tls()) {
            case Tls.Disabled _ -> { /* plaintext http */ }
            case Tls.SystemTrust _ -> { /* https with the JVM default SSLContext */ }
            case Tls.Custom(var certFile, var keyFile, var trustFile) -> {
                Transports.requireCompleteClientMutualTls(certFile, keyFile);
                builder.sslContext(PemSsl.clientContext(certFile, keyFile, trustFile));
            }
        }
        this.http = builder.build();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        log.debug("created OTLP/HTTP client for {}", base);
    }

    /// Wraps an IPv6 literal in brackets to form a valid URL authority; other hosts pass through.
    private static String authority(String host, int port) {
        var h = host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
        return h + ":" + port;
    }

    @Override
    public CompletionStage<ConsumeResult<TracesData>> exportTraces(TracesData traces) {
        return CompletableFuture.supplyAsync(() -> export(
                tracesUri, TraceMapper.toProto(traces).toByteArray(),
                ExportTraceServiceResponse::parseFrom, TraceMapper::result), executor);
    }

    @Override
    public CompletionStage<ConsumeResult<MetricsData>> exportMetrics(MetricsData metrics) {
        return CompletableFuture.supplyAsync(() -> export(
                metricsUri, MetricsMapper.toProto(metrics).toByteArray(),
                ExportMetricsServiceResponse::parseFrom, MetricsMapper::result), executor);
    }

    @Override
    public CompletionStage<ConsumeResult<LogsData>> exportLogs(LogsData logs) {
        return CompletableFuture.supplyAsync(() -> export(
                logsUri, LogsMapper.toProto(logs).toByteArray(),
                ExportLogsServiceResponse::parseFrom, LogsMapper::result), executor);
    }

    @Override
    public CompletionStage<ConsumeResult<ProfilesData>> exportProfiles(ProfilesData profiles) {
        return CompletableFuture.supplyAsync(() -> export(
                profilesUri, ProfilesMapper.toProto(profiles).toByteArray(),
                ExportProfilesServiceResponse::parseFrom, ProfilesMapper::result), executor);
    }

    /// Sends `payload` to `uri`, retrying retryable statuses and IO errors within the [RetryPolicy]
    /// with exponential backoff. A 2xx response is parsed and mapped to [ConsumeResult]; any other
    /// outcome throws, mirroring the gRPC client where a non-OK status is an exception.
    private <RESP, SIG> ConsumeResult<SIG> export(
            URI uri, byte[] payload, ProtoParser<RESP> parse, Function<RESP, ConsumeResult<SIG>> toResult) {
        var retry = config.retry();
        var maxAttempts = Math.max(1, retry.maxAttempts());
        var body = compress ? gzip(payload) : payload;
        var httpRequest = request(uri, body);
        for (var attempt = 1; ; attempt++) {
            HttpResponse<byte[]> response;
            try {
                response = http.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
            } catch (IOException e) {
                if (attempt >= maxAttempts) {
                    throw new UncheckedIOException("OTLP/HTTP export to " + uri + " failed", e);
                }
                sleep(backoffNanos(retry, attempt));
                continue;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CancellationException("interrupted during OTLP/HTTP export to " + uri);
            }

            var code = response.statusCode();
            if (code >= 200 && code < 300) {
                return toResult.apply(parseResponse(parse, response.body()));
            }
            if (RETRYABLE_STATUS.contains(code) && attempt < maxAttempts) {
                sleep(backoffNanos(retry, attempt));
                continue;
            }
            throw new OtlpHttpException(rejection(uri, code, response.body()));
        }
    }

    private HttpRequest request(URI uri, byte[] body) {
        var builder = HttpRequest.newBuilder(uri)
                .timeout(config.timeout())
                .header("Content-Type", OtlpHttp.CONTENT_TYPE);
        if (compress) {
            builder.header("Content-Encoding", "gzip");
        }
        config.headers().forEach(builder::header);
        return builder.POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
    }

    private static <RESP> RESP parseResponse(ProtoParser<RESP> parse, byte[] body) {
        try {
            return parse.parse(body);
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException("malformed OTLP/HTTP response body", e);
        }
    }

    private static String rejection(URI uri, int code, byte[] body) {
        var text = new String(body, StandardCharsets.UTF_8).strip();
        return "OTLP/HTTP export to " + uri + " rejected: HTTP " + code + (text.isEmpty() ? "" : " - " + text);
    }

    /// Exponential backoff for the failed `attempt` (1-based), growing by the policy's
    /// `backoffMultiplier` and capped at `maxBackoff`.
    private static long backoffNanos(RetryPolicy retry, int attempt) {
        var delay = (double) retry.initialBackoff().toNanos();
        var max = retry.maxBackoff().toNanos();
        for (var i = 1; i < attempt; i++) {
            delay = Math.min(max, delay * retry.backoffMultiplier());
        }
        return (long) Math.min(delay, max);
    }

    private static void sleep(long nanos) {
        if (nanos <= 0) {
            return;
        }
        try {
            Thread.sleep(Duration.ofNanos(nanos));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException("interrupted during OTLP/HTTP retry backoff");
        }
    }

    private static byte[] gzip(byte[] data) {
        var out = new ByteArrayOutputStream(data.length);
        try (var gz = new GZIPOutputStream(out)) {
            gz.write(data);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to gzip OTLP/HTTP request body", e);
        }
        return out.toByteArray();
    }

    /// Interrupt-responsive teardown bounded by [#CLOSE_TIMEOUT_SECONDS]: drains the per-export
    /// executor, then forcibly closes the client.
    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("per-export executor did not terminate within {}s; interrupting outstanding exports",
                        CLOSE_TIMEOUT_SECONDS);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            http.shutdownNow();
        }
    }
}
