package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.OtlpClient;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The OTLP/gRPC implementation of the [OtlpClient] SPI: opens a plaintext channel to a
/// collector endpoint and exports domain telemetry asynchronously.
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
    private final Executor executor;

    GrpcOtlpClient(ClientTransportConfig config) {
        this.config = config;
        // TLS variants on the SPI are honoured for `Disabled`; non-disabled values fall back to
        // plaintext in this v1 transport. The TLS-aware path lives here without an SPI break.
        this.channel = Grpc.newChannelBuilderForAddress(
                        config.host(), config.port(), InsecureChannelCredentials.create())
                .build();
        this.traceStub = TraceServiceGrpc.newBlockingStub(channel);
        this.metricsStub = MetricsServiceGrpc.newBlockingStub(channel);
        this.logsStub = LogsServiceGrpc.newBlockingStub(channel);
        this.profilesStub = ProfilesServiceGrpc.newBlockingStub(channel);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        log.debug("opened OTLP/gRPC channel to {}:{}", config.host(), config.port());
    }

    @Override
    public CompletionStage<ConsumeResult<TraceData>> exportTraces(TraceData traces) {
        return CompletableFuture.supplyAsync(() -> TraceMapper.result(
                traceStub.withDeadlineAfter(config.timeout().toNanos(), TimeUnit.NANOSECONDS)
                        .export(TraceMapper.toProto(traces))), executor);
    }

    @Override
    public CompletionStage<ConsumeResult<MetricsData>> exportMetrics(MetricsData metrics) {
        return CompletableFuture.supplyAsync(() -> MetricsMapper.result(
                metricsStub.withDeadlineAfter(config.timeout().toNanos(), TimeUnit.NANOSECONDS)
                        .export(MetricsMapper.toProto(metrics))), executor);
    }

    @Override
    public CompletionStage<ConsumeResult<LogsData>> exportLogs(LogsData logs) {
        return CompletableFuture.supplyAsync(() -> LogsMapper.result(
                logsStub.withDeadlineAfter(config.timeout().toNanos(), TimeUnit.NANOSECONDS)
                        .export(LogsMapper.toProto(logs))), executor);
    }

    @Override
    public CompletionStage<ConsumeResult<ProfilesData>> exportProfiles(ProfilesData profiles) {
        return CompletableFuture.supplyAsync(() -> ProfilesMapper.result(
                profilesStub.withDeadlineAfter(config.timeout().toNanos(), TimeUnit.NANOSECONDS)
                        .export(ProfilesMapper.toProto(profiles))), executor);
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
            // Close the per-export executor so outstanding supplyAsync tasks are interrupted
            // and no carrier threads leak through repeated channel cycles.
            if (executor instanceof ExecutorService es) {
                es.close();
            }
        }
    }
}
