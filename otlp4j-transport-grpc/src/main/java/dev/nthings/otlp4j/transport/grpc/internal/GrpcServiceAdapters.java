package dev.nthings.otlp4j.transport.grpc.internal;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.codec.SignalCodec;
import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.transport.common.AdmissionControl;
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
/// - A retryable [ConsumeResult.Rejected] -> gRPC `UNAVAILABLE`. Use for
///   back-pressure such as a full queue. A `retryAfter` hint is emitted as
///   the `grpc-retry-pushback-ms` trailer, which the bundled client folds
///   into its retry intervals.
/// - A permanent [ConsumeResult.Rejected] -> gRPC `INTERNAL`. Use for a
///   deterministic fault such as content filtering, so the client does not
///   waste retries on a permanently unreachable outcome.
///
/// A `cause`, when present, is attached as the status cause regardless of
/// retryability.
final class GrpcServiceAdapters {

    private static final Logger log = LoggerFactory.getLogger(GrpcServiceAdapters.class);

    private GrpcServiceAdapters() {
    }

    /// Creates the four OTLP collector services, each bound to its
    /// signal's dispatcher, gated by the shared admission control.
    public static List<BindableService> create(Dispatchers dispatchers, AdmissionControl admission) {
        return List.of(
                new TraceServiceAdapter(t -> dispatchers.dispatch(Signal.TRACES.batch(t)), admission),
                new MetricsServiceAdapter(m -> dispatchers.dispatch(Signal.METRICS.batch(m)), admission),
                new LogsServiceAdapter(l -> dispatchers.dispatch(Signal.LOGS.batch(l)), admission),
                new ProfilesServiceAdapter(p -> dispatchers.dispatch(Signal.PROFILES.batch(p)), admission));
    }

    /// Decodes `request` and dispatches to the signal handler, holding an admission permit for
    /// the request's decoded size until the dispatch completes. At capacity the request is
    /// rejected with `UNAVAILABLE` "receiver at capacity" (plus a pushback trailer when
    /// configured). A synchronous failure maps to gRPC `INTERNAL`; the asynchronous outcome is
    /// handed to [#respond].
    static <REQ extends MessageLite, SIG, RESP> void dispatch(
            REQ request,
            StreamObserver<RESP> observer,
            Function<REQ, SIG> toDomain,
            Function<SIG, CompletionStage<ConsumeResult>> dispatcher,
            Function<ConsumeResult, RESP> asResponse,
            AdmissionControl admission) {
        // Lazy sizing: getSerializedSize walks the whole message, so it runs only when a limit
        // is configured.
        var permit = admission.tryAcquire(request::getSerializedSize);
        if (permit == null) {
            observer.onError(atCapacity(admission));
            return;
        }
        CompletionStage<ConsumeResult> stage;
        try {
            stage = dispatcher.apply(toDomain.apply(request));
        } catch (RuntimeException e) {
            permit.close();
            observer.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
            return;
        }
        respond(observer, stage.whenComplete((result, error) -> permit.close()), asResponse);
    }

    /// `UNAVAILABLE` for a request rejected at admission, carrying the configured pushback hint.
    private static Throwable atCapacity(AdmissionControl admission) {
        log.warn("receiver at capacity ({} requests / {} bytes in flight); responding with gRPC UNAVAILABLE",
                admission.inFlightRequests(), admission.inFlightBytes());
        return Status.UNAVAILABLE
                .withDescription("receiver at capacity")
                .asRuntimeException(GrpcPushback.pushbackTrailers(admission.retryAfterHint()));
    }

    /// Asynchronously consumes the result, mapping [ConsumeResult] to a gRPC
    /// response or to an INTERNAL error.
    static <SIG, RESP> void respond(
            StreamObserver<RESP> observer,
            CompletionStage<ConsumeResult> stage,
            Function<ConsumeResult, RESP> mapResult) {
        stage.whenComplete((result, error) -> {
            if (error != null) {
                log.warn("dispatcher failed; responding with gRPC INTERNAL", error);
                observer.onError(Status.INTERNAL
                        .withDescription(error.getMessage())
                        .withCause(error)
                        .asRuntimeException());
                return;
            }
            if (result instanceof ConsumeResult.Rejected(var retryable, var message, var cause, var retryAfter)) {
                var status = retryable ? Status.UNAVAILABLE : Status.INTERNAL;
                if (cause != null) {
                    status = status.withCause(cause);
                }
                log.warn("dispatcher rejected the whole batch; responding with gRPC {}: {}",
                        status.getCode(), message);
                observer.onError(status.withDescription(message)
                        .asRuntimeException(GrpcPushback.pushbackTrailers(retryAfter)));
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

    private final Function<TracesData, CompletionStage<ConsumeResult>> dispatcher;
    private final AdmissionControl admission;

    TraceServiceAdapter(Function<TracesData, CompletionStage<ConsumeResult>> dispatcher, AdmissionControl admission) {
        this.dispatcher = dispatcher;
        this.admission = admission;
    }

    @Override
    public void export(
            ExportTraceServiceRequest request,
            StreamObserver<ExportTraceServiceResponse> observer) {
        GrpcServiceAdapters.dispatch(
                request, observer, SignalCodec.TRACES::toDomain, dispatcher, SignalCodec.TRACES::response, admission);
    }
}

final class MetricsServiceAdapter extends MetricsServiceGrpc.MetricsServiceImplBase {

    private final Function<MetricsData, CompletionStage<ConsumeResult>> dispatcher;
    private final AdmissionControl admission;

    MetricsServiceAdapter(Function<MetricsData, CompletionStage<ConsumeResult>> dispatcher, AdmissionControl admission) {
        this.dispatcher = dispatcher;
        this.admission = admission;
    }

    @Override
    public void export(
            ExportMetricsServiceRequest request,
            StreamObserver<ExportMetricsServiceResponse> observer) {
        GrpcServiceAdapters.dispatch(
                request, observer, SignalCodec.METRICS::toDomain, dispatcher, SignalCodec.METRICS::response, admission);
    }
}

final class LogsServiceAdapter extends LogsServiceGrpc.LogsServiceImplBase {

    private final Function<LogsData, CompletionStage<ConsumeResult>> dispatcher;
    private final AdmissionControl admission;

    LogsServiceAdapter(Function<LogsData, CompletionStage<ConsumeResult>> dispatcher, AdmissionControl admission) {
        this.dispatcher = dispatcher;
        this.admission = admission;
    }

    @Override
    public void export(
            ExportLogsServiceRequest request,
            StreamObserver<ExportLogsServiceResponse> observer) {
        GrpcServiceAdapters.dispatch(
                request, observer, SignalCodec.LOGS::toDomain, dispatcher, SignalCodec.LOGS::response, admission);
    }
}

final class ProfilesServiceAdapter extends ProfilesServiceGrpc.ProfilesServiceImplBase {

    private final Function<ProfilesData, CompletionStage<ConsumeResult>> dispatcher;
    private final AdmissionControl admission;

    ProfilesServiceAdapter(Function<ProfilesData, CompletionStage<ConsumeResult>> dispatcher, AdmissionControl admission) {
        this.dispatcher = dispatcher;
        this.admission = admission;
    }

    @Override
    public void export(
            ExportProfilesServiceRequest request,
            StreamObserver<ExportProfilesServiceResponse> observer) {
        GrpcServiceAdapters.dispatch(
                request, observer, SignalCodec.PROFILES::toDomain, dispatcher, SignalCodec.PROFILES::response, admission);
    }
}
