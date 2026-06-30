package dev.nthings.otlp4j.transport.grpc.internal;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.codec.DeliveryResults;
import dev.nthings.otlp4j.codec.LogsMapper;
import dev.nthings.otlp4j.codec.MetricsMapper;
import dev.nthings.otlp4j.codec.ProfilesMapper;
import dev.nthings.otlp4j.codec.SignalResponses;
import dev.nthings.otlp4j.codec.TraceMapper;
import dev.nthings.otlp4j.spi.Dispatchers;
import io.grpc.BindableService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ProfilesServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Builds gRPC services that decode OTLP requests and dispatch to per-signal handlers.
///
/// The [ConsumeResult] to gRPC status contract drives client retries:
///
/// - [ConsumeResult.Accepted] / [ConsumeResult.Partial] -> normal OTLP
///   response with appropriate `partial_success`.
/// - [ConsumeResult.Rejected] without cause -> gRPC `UNAVAILABLE` (retryable).
///   Use for back-pressure such as a full queue.
/// - [ConsumeResult.Rejected] with cause -> gRPC `INTERNAL` (permanent failure).
///
/// Deterministic rejections (e.g. content filtering) MUST attach a cause so the
/// client does not waste retries on a permanently unreachable outcome.
final class GrpcServiceAdapters {

    private static final Logger log = LoggerFactory.getLogger(GrpcServiceAdapters.class);

    private GrpcServiceAdapters() {
    }

    /// Creates the four OTLP collector services, each bound to its
    /// signal's dispatcher.
    public static List<BindableService> create(Dispatchers dispatchers) {
        return List.of(
                new TraceServiceAdapter(dispatchers.traces()),
                new MetricsServiceAdapter(dispatchers.metrics()),
                new LogsServiceAdapter(dispatchers.logs()),
                new ProfilesServiceAdapter(dispatchers.profiles()));
    }

    /// Decodes `request` and dispatches to the signal handler. A synchronous
    /// failure maps to gRPC `INTERNAL`; the asynchronous outcome is
    /// handed to [#respond].
    static <REQ, SIG, RESP> void dispatch(
            REQ request,
            StreamObserver<RESP> observer,
            Function<REQ, SIG> toDomain,
            Function<SIG, CompletionStage<ConsumeResult<SIG>>> dispatcher,
            Function<ConsumeResult<SIG>, RESP> asResponse) {
        CompletionStage<ConsumeResult<SIG>> stage;
        try {
            stage = dispatcher.apply(toDomain.apply(request));
        } catch (RuntimeException e) {
            observer.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
            return;
        }
        respond(observer, stage, asResponse);
    }

    /// Asynchronously consumes the result, mapping [ConsumeResult] to a gRPC
    /// response or to an INTERNAL error.
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
            if (result instanceof ConsumeResult.Rejected<SIG>(var message, var cause)) {
                var status = DeliveryResults.isRetryable((ConsumeResult.Rejected<?>) result)
                        ? Status.UNAVAILABLE
                        : Status.INTERNAL;
                if (cause != null) {
                    status = status.withCause(cause);
                }
                log.warn("dispatcher rejected the whole batch; responding with gRPC {}: {}",
                        status.getCode(), message);
                observer.onError(status.withDescription(message).asRuntimeException());
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

    private final Function<TracesData, CompletionStage<ConsumeResult<TracesData>>> dispatcher;

    TraceServiceAdapter(Function<TracesData, CompletionStage<ConsumeResult<TracesData>>> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void export(
            ExportTraceServiceRequest request,
            StreamObserver<ExportTraceServiceResponse> observer) {
        GrpcServiceAdapters.dispatch(
                request, observer, TraceMapper::toDomain, dispatcher, SignalResponses::traces);
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
        GrpcServiceAdapters.dispatch(
                request, observer, MetricsMapper::toDomain, dispatcher, SignalResponses::metrics);
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
        GrpcServiceAdapters.dispatch(
                request, observer, LogsMapper::toDomain, dispatcher, SignalResponses::logs);
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
        GrpcServiceAdapters.dispatch(
                request, observer, ProfilesMapper::toDomain, dispatcher, SignalResponses::profiles);
    }
}
