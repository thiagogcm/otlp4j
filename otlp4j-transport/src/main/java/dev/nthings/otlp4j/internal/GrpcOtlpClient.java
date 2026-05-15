package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.spi.OtlpClient;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The OTLP/gRPC implementation of the [OtlpClient] SPI: opens a plaintext channel to a
/// collector endpoint and exports domain telemetry over the four OTLP services.
///
/// **Internal.** Part of the transport layer; obtained via the SPI, never
/// referenced by name from the API or downstream modules.
final class GrpcOtlpClient implements OtlpClient {

    private static final Logger log = LoggerFactory.getLogger(GrpcOtlpClient.class);

    private static final long CLOSE_TIMEOUT_SECONDS = 5;

    private final ManagedChannel channel;
    private final TraceServiceGrpc.TraceServiceBlockingStub traceStub;
    private final MetricsServiceGrpc.MetricsServiceBlockingStub metricsStub;
    private final LogsServiceGrpc.LogsServiceBlockingStub logsStub;
    private final ProfilesServiceGrpc.ProfilesServiceBlockingStub profilesStub;
    private final Duration timeout;

    GrpcOtlpClient(String host, int port, Duration timeout) {
        this.channel =
                Grpc.newChannelBuilderForAddress(host, port, InsecureChannelCredentials.create())
                        .build();
        this.traceStub = TraceServiceGrpc.newBlockingStub(channel);
        this.metricsStub = MetricsServiceGrpc.newBlockingStub(channel);
        this.logsStub = LogsServiceGrpc.newBlockingStub(channel);
        this.profilesStub = ProfilesServiceGrpc.newBlockingStub(channel);
        this.timeout = timeout;
        log.debug("opened OTLP/gRPC channel to {}:{}", host, port);
    }

    @Override
    public ExportResult exportTraces(TraceData traces) {
        return TraceMapper.result(
                traceStub.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
                        .export(TraceMapper.toProto(traces)));
    }

    @Override
    public ExportResult exportMetrics(MetricsData metrics) {
        return MetricsMapper.result(
                metricsStub.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
                        .export(MetricsMapper.toProto(metrics)));
    }

    @Override
    public ExportResult exportLogs(LogsData logs) {
        return LogsMapper.result(
                logsStub.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
                        .export(LogsMapper.toProto(logs)));
    }

    @Override
    public ExportResult exportProfiles(ProfilesData profiles) {
        return ProfilesMapper.result(
                profilesStub.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
                        .export(ProfilesMapper.toProto(profiles)));
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
        }
    }
}
