package dev.nthings.otlp4j.transport.grpc.internal;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.codec.DeliveryResultStatuses;
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
/// The [DeliveryResult] to gRPC status contract drives client retries:
///
/// - [DeliveryResult.Accepted] / [DeliveryResult.Partial] -> normal OTLP
///   response with appropriate `partial_success`.
/// - A retryable [DeliveryResult.Rejected] -> gRPC `UNAVAILABLE`. Use for
///   back-pressure such as a full queue. A `retryAfter` hint is emitted as
///   the `grpc-retry-pushback-ms` trailer, which the bundled client folds
///   into its retry intervals.
/// - A permanent [DeliveryResult.Rejected] -> gRPC `INTERNAL`. Use for a
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
                new TraceServiceAdapter(
                        request -> dispatchers.dispatch(SignalCodec.TRACES.proto(request)), admission),
                new MetricsServiceAdapter(
                        request -> dispatchers.dispatch(SignalCodec.METRICS.proto(request)), admission),
                new LogsServiceAdapter(
                        request -> dispatchers.dispatch(SignalCodec.LOGS.proto(request)), admission),
                new ProfilesServiceAdapter(
                        request -> dispatchers.dispatch(SignalCodec.PROFILES.proto(request)), admission));
    }

    /// Decodes `request` and dispatches to the signal handler, holding an admission permit for
    /// the request's decoded size until the dispatch completes. At capacity the request is
    /// rejected with `UNAVAILABLE` "receiver at capacity" (plus a pushback trailer when
    /// configured). A synchronous failure maps to gRPC `INTERNAL`; the asynchronous outcome is
    /// handed to [#respond].
    static <REQ extends MessageLite, RESP> void dispatch(
            REQ request,
            StreamObserver<RESP> observer,
            Function<REQ, CompletionStage<DeliveryResult>> dispatcher,
            Function<DeliveryResult, RESP> asResponse,
            AdmissionControl admission) {
        // Lazy sizing: getSerializedSize walks the whole message, so it runs only when a limit
        // is configured.
        var permit = admission.tryAcquire(request::getSerializedSize);
        if (permit == null) {
            observer.onError(atCapacity(admission));
            return;
        }
        CompletionStage<DeliveryResult> stage;
        try {
            stage = dispatcher.apply(request);
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

    /// Asynchronously consumes the result, mapping [DeliveryResult] to a gRPC
    /// response or to an INTERNAL error.
    static <SIG, RESP> void respond(
            StreamObserver<RESP> observer,
            CompletionStage<DeliveryResult> stage,
            Function<DeliveryResult, RESP> mapResult) {
        stage.whenComplete((result, error) -> {
            if (error != null) {
                log.warn("dispatcher failed; responding with gRPC INTERNAL", error);
                observer.onError(Status.INTERNAL
                        .withDescription(error.getMessage())
                        .withCause(error)
                        .asRuntimeException());
                return;
            }
            if (result instanceof DeliveryResult.Rejected rejected) {
                var status = DeliveryResultStatuses.isRetryable(rejected) ? Status.UNAVAILABLE : Status.INTERNAL;
                if (rejected.cause() != null) {
                    status = status.withCause(rejected.cause());
                }
                log.warn("dispatcher rejected the whole batch; responding with gRPC {}: {}",
                        status.getCode(), rejected.message());
                observer.onError(status.withDescription(rejected.message())
                        .asRuntimeException(GrpcPushback.pushbackTrailers(rejected.retryAfter())));
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

    private final Function<ExportTraceServiceRequest, CompletionStage<DeliveryResult>> dispatcher;
    private final AdmissionControl admission;

    TraceServiceAdapter(
            Function<ExportTraceServiceRequest, CompletionStage<DeliveryResult>> dispatcher,
            AdmissionControl admission) {
        this.dispatcher = dispatcher;
        this.admission = admission;
    }

    @Override
    public void export(
            ExportTraceServiceRequest request,
            StreamObserver<ExportTraceServiceResponse> observer) {
        GrpcServiceAdapters.dispatch(
                request, observer, dispatcher, SignalCodec.TRACES::deliveryResponse, admission);
    }
}

final class MetricsServiceAdapter extends MetricsServiceGrpc.MetricsServiceImplBase {

    private final Function<ExportMetricsServiceRequest, CompletionStage<DeliveryResult>> dispatcher;
    private final AdmissionControl admission;

    MetricsServiceAdapter(
            Function<ExportMetricsServiceRequest, CompletionStage<DeliveryResult>> dispatcher,
            AdmissionControl admission) {
        this.dispatcher = dispatcher;
        this.admission = admission;
    }

    @Override
    public void export(
            ExportMetricsServiceRequest request,
            StreamObserver<ExportMetricsServiceResponse> observer) {
        GrpcServiceAdapters.dispatch(
                request, observer, dispatcher, SignalCodec.METRICS::deliveryResponse, admission);
    }
}

final class LogsServiceAdapter extends LogsServiceGrpc.LogsServiceImplBase {

    private final Function<ExportLogsServiceRequest, CompletionStage<DeliveryResult>> dispatcher;
    private final AdmissionControl admission;

    LogsServiceAdapter(
            Function<ExportLogsServiceRequest, CompletionStage<DeliveryResult>> dispatcher,
            AdmissionControl admission) {
        this.dispatcher = dispatcher;
        this.admission = admission;
    }

    @Override
    public void export(
            ExportLogsServiceRequest request,
            StreamObserver<ExportLogsServiceResponse> observer) {
        GrpcServiceAdapters.dispatch(
                request, observer, dispatcher, SignalCodec.LOGS::deliveryResponse, admission);
    }
}

final class ProfilesServiceAdapter extends ProfilesServiceGrpc.ProfilesServiceImplBase {

    private final Function<ExportProfilesServiceRequest, CompletionStage<DeliveryResult>> dispatcher;
    private final AdmissionControl admission;

    ProfilesServiceAdapter(
            Function<ExportProfilesServiceRequest, CompletionStage<DeliveryResult>> dispatcher,
            AdmissionControl admission) {
        this.dispatcher = dispatcher;
        this.admission = admission;
    }

    @Override
    public void export(
            ExportProfilesServiceRequest request,
            StreamObserver<ExportProfilesServiceResponse> observer) {
        GrpcServiceAdapters.dispatch(
                request, observer, dispatcher, SignalCodec.PROFILES::deliveryResponse, admission);
    }
}
