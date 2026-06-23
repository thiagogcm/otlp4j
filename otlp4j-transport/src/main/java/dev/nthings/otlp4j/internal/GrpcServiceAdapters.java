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

/// Builds gRPC services that decode OTLP requests and call the four per-signal dispatchers.
///
/// The [ConsumeResult] -> gRPC status contract is deliberate and load-bearing for client retries:
///
///   - [ConsumeResult.Accepted] / [ConsumeResult.Partial] -> OTLP `partial_success` on a normal
///     response. (`Accepted` leaves `partial_success` unset; `Partial` carries the rejected count
///     and message.)
///   - A whole-batch [ConsumeResult.Rejected] is NOT a partial success — encoding it as
///     `rejected_*=0` would read to the client as "all accepted" — so it always maps to a gRPC
///     error, never a response message:
///       - Rejected with NO cause => gRPC `UNAVAILABLE`. By design this means "transient/retryable":
///         a well-behaved OTLP client will retry within its budget. Use it for back-pressure such as
///         a full queue.
///       - Rejected WITH a cause => gRPC `INTERNAL`, a non-retryable fault (same mapping as a thrown
///         exception). The cause is the signal that this is a permanent failure.
///
/// Consequence for deterministic/permanent rejections: a dispatcher that will reject the SAME batch
/// every time (e.g. a content filter dropping disallowed data) MUST attach a cause so it maps to
/// `INTERNAL`. A no-cause `UNAVAILABLE` would otherwise be retried until the client's budget is
/// exhausted — wasted work for an outcome that can never change. "No cause == retryable" is the
/// contract; permanent failures opt out by carrying a cause.
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

    /// Decodes `request` and runs the signal's `dispatcher`. A synchronous decode/dispatch failure
    /// maps to gRPC `INTERNAL`; the asynchronous outcome is handed to [#respond].
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
            if (result instanceof ConsumeResult.Rejected<SIG>(var message, var cause)) {
                // Whole-batch rejection is a delivery failure, not a partial success: a gRPC error so
                // the client retries (UNAVAILABLE) or sees the fault (INTERNAL), not rejected_*=0.
                var status = (cause == null ? Status.UNAVAILABLE : Status.INTERNAL).withDescription(message);
                if (cause != null) {
                    status = status.withCause(cause);
                }
                log.warn("dispatcher rejected the whole batch; responding with gRPC {}: {}",
                        status.getCode(), message);
                observer.onError(status.asRuntimeException());
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
