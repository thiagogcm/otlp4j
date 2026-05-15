package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import io.grpc.BindableService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesPartialSuccess;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Builds gRPC services that decode OTLP requests and call a [TelemetryConsumer].
///
/// Adapters map consumer [ExportResult] values to OTLP `partial_success`; thrown exceptions become
/// gRPC `INTERNAL` errors.
final class GrpcServiceAdapters {

    private static final Logger log = LoggerFactory.getLogger(GrpcServiceAdapters.class);

    private GrpcServiceAdapters() {}

    /// Creates the four OTLP collector services, all feeding the given consumer.
    public static List<BindableService> create(TelemetryConsumer consumer) {
        return List.of(
                new TraceServiceAdapter(consumer),
                new MetricsServiceAdapter(consumer),
                new LogsServiceAdapter(consumer),
                new ProfilesServiceAdapter(consumer));
    }

    /// Runs `work`, streaming its result or translating a thrown exception into a gRPC error.
    static <RESP> void respond(StreamObserver<RESP> observer, Supplier<RESP> work) {
        try {
            observer.onNext(work.get());
            observer.onCompleted();
        } catch (RuntimeException e) {
            log.warn("telemetry consumer failed; responding with gRPC INTERNAL", e);
            observer.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }
}

final class TraceServiceAdapter extends TraceServiceGrpc.TraceServiceImplBase {

    private final TelemetryConsumer consumer;

    TraceServiceAdapter(TelemetryConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void export(
            ExportTraceServiceRequest request,
            StreamObserver<ExportTraceServiceResponse> observer) {
        GrpcServiceAdapters.respond(observer, () -> {
            var result = consumer.consumeTraces(TraceMapper.toDomain(request));
            var response = ExportTraceServiceResponse.newBuilder();
            if (!result.isFullSuccess()) {
                response.setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(result.rejectedCount())
                        .setErrorMessage(result.message()));
            }
            return response.build();
        });
    }
}

final class MetricsServiceAdapter extends MetricsServiceGrpc.MetricsServiceImplBase {

    private final TelemetryConsumer consumer;

    MetricsServiceAdapter(TelemetryConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void export(
            ExportMetricsServiceRequest request,
            StreamObserver<ExportMetricsServiceResponse> observer) {
        GrpcServiceAdapters.respond(observer, () -> {
            var result = consumer.consumeMetrics(MetricsMapper.toDomain(request));
            var response = ExportMetricsServiceResponse.newBuilder();
            if (!result.isFullSuccess()) {
                response.setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                        .setRejectedDataPoints(result.rejectedCount())
                        .setErrorMessage(result.message()));
            }
            return response.build();
        });
    }
}

final class LogsServiceAdapter extends LogsServiceGrpc.LogsServiceImplBase {

    private final TelemetryConsumer consumer;

    LogsServiceAdapter(TelemetryConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void export(
            ExportLogsServiceRequest request,
            StreamObserver<ExportLogsServiceResponse> observer) {
        GrpcServiceAdapters.respond(observer, () -> {
            var result = consumer.consumeLogs(LogsMapper.toDomain(request));
            var response = ExportLogsServiceResponse.newBuilder();
            if (!result.isFullSuccess()) {
                response.setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                        .setRejectedLogRecords(result.rejectedCount())
                        .setErrorMessage(result.message()));
            }
            return response.build();
        });
    }
}

final class ProfilesServiceAdapter extends ProfilesServiceGrpc.ProfilesServiceImplBase {

    private final TelemetryConsumer consumer;

    ProfilesServiceAdapter(TelemetryConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void export(
            ExportProfilesServiceRequest request,
            StreamObserver<ExportProfilesServiceResponse> observer) {
        GrpcServiceAdapters.respond(observer, () -> {
            var result = consumer.consumeProfiles(ProfilesMapper.toDomain(request));
            var response = ExportProfilesServiceResponse.newBuilder();
            if (!result.isFullSuccess()) {
                response.setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                        .setRejectedProfiles(result.rejectedCount())
                        .setErrorMessage(result.message()));
            }
            return response.build();
        });
    }
}
