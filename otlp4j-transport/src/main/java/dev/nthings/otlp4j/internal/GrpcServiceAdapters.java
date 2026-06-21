package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
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
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Builds gRPC services that decode OTLP requests and call the four per-signal dispatchers.
///
/// Each adapter maps the dispatcher's [ConsumeResult] to OTLP `partial_success`; thrown
/// exceptions become gRPC `INTERNAL`.
final class GrpcServiceAdapters {

    private static final Logger log = LoggerFactory.getLogger(GrpcServiceAdapters.class);

    private GrpcServiceAdapters() {}

    /// Creates the four OTLP collector services, each bound to its signal's dispatcher.
    public static List<BindableService> create(OtlpServerProvider.Dispatchers dispatchers) {
        return List.of(
                new TraceServiceAdapter(dispatchers.traces()),
                new MetricsServiceAdapter(dispatchers.metrics()),
                new LogsServiceAdapter(dispatchers.logs()),
                new ProfilesServiceAdapter(dispatchers.profiles()));
    }

    /// Asynchronously consumes `request`, mapping the eventual [ConsumeResult] back to the
    /// gRPC response (via `partial`) or to an INTERNAL error.
    static <SIG, RESP> void respond(
            StreamObserver<RESP> observer,
            CompletionStage<ConsumeResult<SIG>> stage,
            Function<ConsumeResult<SIG>, RESP> mapResult) {
        stage.whenComplete((result, error) -> {
            if (error != null) {
                log.warn("dispatcher failed; responding with gRPC INTERNAL", error);
                observer.onError(Status.INTERNAL
                        .withDescription(error.getMessage())
                        .withCause(error)
                        .asRuntimeException());
                return;
            }
            try {
                observer.onNext(mapResult.apply(result));
                observer.onCompleted();
            } catch (RuntimeException e) {
                observer.onError(Status.INTERNAL
                        .withDescription(e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
            }
        });
    }
}

final class TraceServiceAdapter extends TraceServiceGrpc.TraceServiceImplBase {

    private final Function<TraceData, CompletionStage<ConsumeResult<TraceData>>> dispatcher;

    TraceServiceAdapter(Function<TraceData, CompletionStage<ConsumeResult<TraceData>>> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void export(
            ExportTraceServiceRequest request,
            StreamObserver<ExportTraceServiceResponse> observer) {
        CompletionStage<ConsumeResult<TraceData>> stage;
        try {
            stage = dispatcher.apply(TraceMapper.toDomain(request));
        } catch (RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
            return;
        }
        GrpcServiceAdapters.respond(observer, stage, TraceServiceAdapter::asResponse);
    }

    private static ExportTraceServiceResponse asResponse(ConsumeResult<TraceData> result) {
        var resp = ExportTraceServiceResponse.newBuilder();
        switch (result) {
            case ConsumeResult.Accepted<TraceData> _ -> { }
            case ConsumeResult.Partial<TraceData>(var rejected, var message) ->
                    resp.setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                            .setRejectedSpans(rejected)
                            .setErrorMessage(message));
            case ConsumeResult.Rejected<TraceData>(var message, var _) ->
                    resp.setPartialSuccess(ExportTracePartialSuccess.newBuilder().setErrorMessage(message));
        }
        return resp.build();
    }
}

final class MetricsServiceAdapter extends MetricsServiceGrpc.MetricsServiceImplBase {

    private final Function<MetricsData, CompletionStage<ConsumeResult<MetricsData>>> dispatcher;

    MetricsServiceAdapter(Function<MetricsData, CompletionStage<ConsumeResult<MetricsData>>> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void export(
            ExportMetricsServiceRequest request,
            StreamObserver<ExportMetricsServiceResponse> observer) {
        CompletionStage<ConsumeResult<MetricsData>> stage;
        try {
            stage = dispatcher.apply(MetricsMapper.toDomain(request));
        } catch (RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
            return;
        }
        GrpcServiceAdapters.respond(observer, stage, MetricsServiceAdapter::asResponse);
    }

    private static ExportMetricsServiceResponse asResponse(ConsumeResult<MetricsData> result) {
        var resp = ExportMetricsServiceResponse.newBuilder();
        switch (result) {
            case ConsumeResult.Accepted<MetricsData> _ -> { }
            case ConsumeResult.Partial<MetricsData>(var rejected, var message) ->
                    resp.setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                            .setRejectedDataPoints(rejected)
                            .setErrorMessage(message));
            case ConsumeResult.Rejected<MetricsData>(var message, var _) ->
                    resp.setPartialSuccess(ExportMetricsPartialSuccess.newBuilder().setErrorMessage(message));
        }
        return resp.build();
    }
}

final class LogsServiceAdapter extends LogsServiceGrpc.LogsServiceImplBase {

    private final Function<LogsData, CompletionStage<ConsumeResult<LogsData>>> dispatcher;

    LogsServiceAdapter(Function<LogsData, CompletionStage<ConsumeResult<LogsData>>> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void export(
            ExportLogsServiceRequest request,
            StreamObserver<ExportLogsServiceResponse> observer) {
        CompletionStage<ConsumeResult<LogsData>> stage;
        try {
            stage = dispatcher.apply(LogsMapper.toDomain(request));
        } catch (RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
            return;
        }
        GrpcServiceAdapters.respond(observer, stage, LogsServiceAdapter::asResponse);
    }

    private static ExportLogsServiceResponse asResponse(ConsumeResult<LogsData> result) {
        var resp = ExportLogsServiceResponse.newBuilder();
        switch (result) {
            case ConsumeResult.Accepted<LogsData> _ -> { }
            case ConsumeResult.Partial<LogsData>(var rejected, var message) ->
                    resp.setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                            .setRejectedLogRecords(rejected)
                            .setErrorMessage(message));
            case ConsumeResult.Rejected<LogsData>(var message, var _) ->
                    resp.setPartialSuccess(ExportLogsPartialSuccess.newBuilder().setErrorMessage(message));
        }
        return resp.build();
    }
}

final class ProfilesServiceAdapter extends ProfilesServiceGrpc.ProfilesServiceImplBase {

    private final Function<ProfilesData, CompletionStage<ConsumeResult<ProfilesData>>> dispatcher;

    ProfilesServiceAdapter(Function<ProfilesData, CompletionStage<ConsumeResult<ProfilesData>>> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void export(
            ExportProfilesServiceRequest request,
            StreamObserver<ExportProfilesServiceResponse> observer) {
        CompletionStage<ConsumeResult<ProfilesData>> stage;
        try {
            stage = dispatcher.apply(ProfilesMapper.toDomain(request));
        } catch (RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
            return;
        }
        GrpcServiceAdapters.respond(observer, stage, ProfilesServiceAdapter::asResponse);
    }

    private static ExportProfilesServiceResponse asResponse(ConsumeResult<ProfilesData> result) {
        var resp = ExportProfilesServiceResponse.newBuilder();
        switch (result) {
            case ConsumeResult.Accepted<ProfilesData> _ -> { }
            case ConsumeResult.Partial<ProfilesData>(var rejected, var message) ->
                    resp.setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                            .setRejectedProfiles(rejected)
                            .setErrorMessage(message));
            case ConsumeResult.Rejected<ProfilesData>(var message, var _) ->
                    resp.setPartialSuccess(ExportProfilesPartialSuccess.newBuilder().setErrorMessage(message));
        }
        return resp.build();
    }
}
