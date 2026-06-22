package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.Compression;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.spi.Tls;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The OTLP/gRPC implementation of the [OtlpClient] SPI: opens a channel to a collector
/// endpoint and exports domain telemetry asynchronously.
///
/// Honours the full [ClientTransportConfig]: TLS credentials, per-call headers, gzip, and retry.
///
/// **Internal.** Part of the transport layer; obtained via the SPI.
final class GrpcOtlpClient implements OtlpClient {

    private static final Logger log = LoggerFactory.getLogger(GrpcOtlpClient.class);

    private static final long CLOSE_TIMEOUT_SECONDS = 5;

    private final ManagedChannel channel;
    private final TraceServiceGrpc.TraceServiceBlockingStub traceStub;
    private final MetricsServiceGrpc.MetricsServiceBlockingStub metricsStub;
    private final LogsServiceGrpc.LogsServiceBlockingStub logsStub;
    private final ProfilesServiceGrpc.ProfilesServiceBlockingStub profilesStub;
    private final ClientTransportConfig config;
    private final boolean compress;
    private final ExecutorService executor;

    GrpcOtlpClient(ClientTransportConfig config) {
        this.config = config;
        this.compress = config.compression() == Compression.GZIP;

        var builder = Grpc.newChannelBuilderForAddress(
                config.host(), config.port(), channelCredentials(config.tls()));
        if (!config.headers().isEmpty()) {
            builder.intercept(MetadataUtils.newAttachHeadersInterceptor(toMetadata(config.headers())));
        }
        var retry = config.retry();
        if (retry.maxAttempts() > 1) {
            // gRPC's native retry bounds total attempts; the per-call deadline still caps wall time.
            builder.enableRetry()
                    .maxRetryAttempts(retry.maxAttempts())
                    .defaultServiceConfig(RetryServiceConfig.build(retry, OTLP_SERVICE_NAMES));
        }
        this.channel = builder.build();

        this.traceStub = TraceServiceGrpc.newBlockingStub(channel);
        this.metricsStub = MetricsServiceGrpc.newBlockingStub(channel);
        this.logsStub = LogsServiceGrpc.newBlockingStub(channel);
        this.profilesStub = ProfilesServiceGrpc.newBlockingStub(channel);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        log.debug("opened OTLP/gRPC channel to {}:{}", config.host(), config.port());
    }

    @Override
    public CompletionStage<ConsumeResult<TraceData>> exportTraces(TraceData traces) {
        return CompletableFuture.supplyAsync(
                () -> TraceMapper.result(call(traceStub).export(TraceMapper.toProto(traces))), executor);
    }

    @Override
    public CompletionStage<ConsumeResult<MetricsData>> exportMetrics(MetricsData metrics) {
        return CompletableFuture.supplyAsync(
                () -> MetricsMapper.result(call(metricsStub).export(MetricsMapper.toProto(metrics))), executor);
    }

    @Override
    public CompletionStage<ConsumeResult<LogsData>> exportLogs(LogsData logs) {
        return CompletableFuture.supplyAsync(
                () -> LogsMapper.result(call(logsStub).export(LogsMapper.toProto(logs))), executor);
    }

    @Override
    public CompletionStage<ConsumeResult<ProfilesData>> exportProfiles(ProfilesData profiles) {
        return CompletableFuture.supplyAsync(
                () -> ProfilesMapper.result(call(profilesStub).export(ProfilesMapper.toProto(profiles))), executor);
    }

    /// Applies the per-call deadline and, when enabled, gzip compression to a stub.
    private <S extends AbstractStub<S>> S call(S stub) {
        var s = stub.withDeadlineAfter(config.timeout().toNanos(), TimeUnit.NANOSECONDS);
        return compress ? s.withCompression("gzip") : s;
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("OTLP/gRPC channel did not terminate within {}s; forcing shutdown",
                        CLOSE_TIMEOUT_SECONDS);
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("interrupted while closing OTLP/gRPC channel; forcing shutdown");
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            // Bound teardown to the close budget: ExecutorService.close() awaits up to a day, so
            // shut down, wait CLOSE_TIMEOUT_SECONDS, then shutdownNow() so no export carrier leaks.
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
            }
        }
    }

    /// The fully-qualified OTLP collector service names, used to scope the retry service config.
    private static final List<String> OTLP_SERVICE_NAMES = List.of(
            TraceServiceGrpc.SERVICE_NAME,
            MetricsServiceGrpc.SERVICE_NAME,
            LogsServiceGrpc.SERVICE_NAME,
            ProfilesServiceGrpc.SERVICE_NAME);

    private static ChannelCredentials channelCredentials(Tls tls) {
        return switch (tls) {
            case Tls.Disabled() -> InsecureChannelCredentials.create();
            case Tls.SystemTrust() -> TlsChannelCredentials.create();
            case Tls.Custom(var certFile, var keyFile, var trustFile) -> {
                // Cert and key only mean anything together; reject a half-specified pair instead
                // of silently connecting anonymously.
                if ((certFile == null) != (keyFile == null)) {
                    throw new IllegalArgumentException(
                            "incomplete client mutual-TLS material: a certificate and key must be "
                                    + "supplied together (got "
                                    + (certFile != null ? "a certificate without a key" : "a key without a certificate")
                                    + ")");
                }
                try {
                    var b = TlsChannelCredentials.newBuilder();
                    if (certFile != null && keyFile != null) {
                        b.keyManager(certFile.toFile(), keyFile.toFile());
                    }
                    if (trustFile != null) {
                        b.trustManager(trustFile.toFile());
                    }
                    yield b.build();
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to load client TLS material", e);
                }
            }
        };
    }

    private static Metadata toMetadata(Map<String, String> headers) {
        var md = new Metadata();
        headers.forEach((name, value) ->
                md.put(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER), value));
        return md;
    }
}
