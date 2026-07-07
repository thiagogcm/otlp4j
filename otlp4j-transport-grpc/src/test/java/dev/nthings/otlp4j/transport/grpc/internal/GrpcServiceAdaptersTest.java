package dev.nthings.otlp4j.transport.grpc.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.codec.LogsMapper;
import dev.nthings.otlp4j.codec.MetricsMapper;
import dev.nthings.otlp4j.codec.ProfilesMapper;
import dev.nthings.otlp4j.codec.TraceMapper;
import dev.nthings.otlp4j.testing.TransportFixtures;
import dev.nthings.otlp4j.transport.common.AdmissionControl;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// White-box tests for gRPC service adapters without a server or channel.
@DisplayName("gRPC service adapters")
class GrpcServiceAdaptersTest {

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);

    @DisplayName("TraceServiceAdapter forwards the parsed request to the dispatcher")
    @Test
    void traceAdapterForwardsTheParsedRequestToTheDispatcher() {
        var sent = TransportFixtures.richTraceData();
        var received = new AtomicReference<ExportTraceServiceRequest>();
        var observer = new RecordingObserver<ExportTraceServiceResponse>();

        new TraceServiceAdapter(request -> {
            received.set(request);
            return CompletableFuture.completedStage(DeliveryResult.accepted(PROV));
        }, AdmissionControl.unlimited()).export(TraceMapper.toProto(sent), observer);

        observer.await();
        assertThat(received.get()).isEqualTo(TraceMapper.toProto(sent));
        assertThat(observer.completed).isTrue();
        assertThat(observer.error).isNull();
        assertThat(observer.values.getFirst().hasPartialSuccess()).isFalse();
    }

    @DisplayName("TraceServiceAdapter encodes rejected spans as partial success")
    @Test
    void traceAdapterEncodesPartialSuccessOntoTheResponse() {
        var observer = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                DeliveryResult.partial(2L, "2 spans bad", PROV)), AdmissionControl.unlimited()).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), observer);
        observer.await();
        var partial = observer.values.getFirst().getPartialSuccess();
        assertThat(partial.getRejectedSpans()).isEqualTo(2);
        assertThat(partial.getErrorMessage()).isEqualTo("2 spans bad");
        assertThat(observer.completed).isTrue();
    }

    @DisplayName("A retryable whole-batch Rejected maps to UNAVAILABLE, not rejected_spans=0")
    @Test
    void traceAdapterMapsRejectedToUnavailable() {
        var observer = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                DeliveryResult.retryable("queue full", PROV)), AdmissionControl.unlimited()).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), observer);
        observer.await();
        assertThat(observer.values).isEmpty();
        assertThat(observer.completed).isFalse();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(status.getDescription()).isEqualTo("queue full");
    }

    @DisplayName("A throttled Rejected emits the grpc-retry-pushback-ms trailer")
    @Test
    void throttledRejectedEmitsPushbackTrailer() {
        var observer = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                DeliveryResult.throttled(Duration.ofSeconds(2), "at capacity", PROV)), AdmissionControl.unlimited()).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), observer);
        observer.await();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(status.getDescription()).isEqualTo("at capacity");
        var trailers = Status.trailersFromThrowable(observer.error);
        assertThat(trailers).isNotNull();
        assertThat(trailers.get(GrpcPushback.PUSHBACK_MS)).isEqualTo("2000");
    }

    @DisplayName("A retryable Rejected without a hint emits no pushback trailer")
    @Test
    void retryableWithoutHintEmitsNoPushbackTrailer() {
        var observer = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                DeliveryResult.retryable("queue full", PROV)), AdmissionControl.unlimited()).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), observer);
        observer.await();
        var trailers = Status.trailersFromThrowable(observer.error);
        assertThat(trailers == null ? null : trailers.get(GrpcPushback.PUSHBACK_MS)).isNull();
    }

    @DisplayName("A permanent whole-batch Rejected maps to INTERNAL")
    @Test
    void permanentRejectedMapsToInternal() {
        var observer = new RecordingObserver<ExportLogsServiceResponse>();
        new LogsServiceAdapter(logs -> CompletableFuture.completedStage(
                DeliveryResult.permanent("stage threw", new IllegalStateException("boom"), PROV)), AdmissionControl.unlimited()).export(
                        LogsMapper.toProto(TransportFixtures.richLogsData()), observer);
        observer.await();
        assertThat(observer.values).isEmpty();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("stage threw");
    }

    /// Pins the retryability contract: retry intent is stated explicitly, independent of whether a
    /// diagnostic cause is present. A transient rejection that happens to carry a cause is still
    /// UNAVAILABLE (retried), so a briefly-down downstream does not turn into silent data loss.
    @DisplayName("Retryability is explicit, not decided by the presence of a cause")
    @Test
    void retryabilityIsExplicitNotDecidedByCause() {
        var retryableWithCause = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                DeliveryResult.retryable("downstream down", new IllegalStateException("connect timed out"), PROV)), AdmissionControl.unlimited()).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), retryableWithCause);
        retryableWithCause.await();
        assertThat(Status.fromThrowable(retryableWithCause.error).getCode()).isEqualTo(Status.Code.UNAVAILABLE);

        var permanentNoCause = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                DeliveryResult.permanent("dropped by policy", PROV)), AdmissionControl.unlimited()).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), permanentNoCause);
        permanentNoCause.await();
        assertThat(Status.fromThrowable(permanentNoCause.error).getCode()).isEqualTo(Status.Code.INTERNAL);
    }

    @DisplayName("MetricsServiceAdapter encodes rejected data points as partial success")
    @Test
    void metricsAdapterEncodesPartialSuccessOntoTheResponse() {
        var observer = new RecordingObserver<ExportMetricsServiceResponse>();
        new MetricsServiceAdapter(metrics -> CompletableFuture.completedStage(
                DeliveryResult.partial(5L, "5 points bad", PROV)), AdmissionControl.unlimited()).export(
                        MetricsMapper.toProto(TransportFixtures.richMetricsData()), observer);
        observer.await();
        var partial = observer.values.getFirst().getPartialSuccess();
        assertThat(partial.getRejectedDataPoints()).isEqualTo(5);
        assertThat(partial.getErrorMessage()).isEqualTo("5 points bad");
        assertThat(observer.completed).isTrue();
    }

    @DisplayName("LogsServiceAdapter encodes rejected log records as partial success")
    @Test
    void logsAdapterEncodesPartialSuccessOntoTheResponse() {
        var observer = new RecordingObserver<ExportLogsServiceResponse>();
        new LogsServiceAdapter(logs -> CompletableFuture.completedStage(
                DeliveryResult.partial(3L, "3 records bad", PROV)), AdmissionControl.unlimited()).export(
                        LogsMapper.toProto(TransportFixtures.richLogsData()), observer);
        observer.await();
        var partial = observer.values.getFirst().getPartialSuccess();
        assertThat(partial.getRejectedLogRecords()).isEqualTo(3);
        assertThat(partial.getErrorMessage()).isEqualTo("3 records bad");
        assertThat(observer.completed).isTrue();
    }

    @DisplayName("ProfilesServiceAdapter forwards the parsed request to the dispatcher")
    @Test
    void profilesAdapterForwardsTheParsedRequestToTheDispatcher() {
        var received = new AtomicReference<ExportProfilesServiceRequest>();
        var observer = new RecordingObserver<ExportProfilesServiceResponse>();
        new ProfilesServiceAdapter(request -> {
            received.set(request);
            return CompletableFuture.completedStage(DeliveryResult.accepted(PROV));
        }, AdmissionControl.unlimited()).export(ProfilesMapper.toProto(TransportFixtures.profilesData()), observer);
        observer.await();
        assertThat(received.get()).isNotNull();
        assertThat(observer.completed).isTrue();
        assertThat(observer.values.getFirst().hasPartialSuccess()).isFalse();
    }

    @DisplayName("Throwing trace dispatcher becomes an INTERNAL gRPC error")
    @Test
    void aThrowingTraceDispatcherIsTranslatedIntoAnInternalGrpcError() {
        var observer = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> {
            throw new IllegalStateException("handler boom");
        }, AdmissionControl.unlimited()).export(TraceMapper.toProto(TransportFixtures.richTraceData()), observer);
        observer.await();
        assertThat(observer.values).isEmpty();
        assertThat(observer.completed).isFalse();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("handler boom");
    }

    @DisplayName("Throwing metrics dispatcher becomes an INTERNAL gRPC error")
    @Test
    void aThrowingMetricsDispatcherIsTranslatedIntoAnInternalGrpcError() {
        var observer = new RecordingObserver<ExportMetricsServiceResponse>();
        new MetricsServiceAdapter(metrics -> {
            throw new IllegalStateException("metrics boom");
        }, AdmissionControl.unlimited()).export(MetricsMapper.toProto(TransportFixtures.richMetricsData()), observer);
        observer.await();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("metrics boom");
    }

    @DisplayName("Throwing logs dispatcher becomes an INTERNAL gRPC error")
    @Test
    void aThrowingLogsDispatcherIsTranslatedIntoAnInternalGrpcError() {
        var observer = new RecordingObserver<ExportLogsServiceResponse>();
        new LogsServiceAdapter(logs -> {
            throw new IllegalStateException("logs boom");
        }, AdmissionControl.unlimited()).export(LogsMapper.toProto(TransportFixtures.richLogsData()), observer);
        observer.await();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("logs boom");
    }

    @DisplayName("Throwing profiles dispatcher becomes an INTERNAL gRPC error")
    @Test
    void aThrowingProfilesDispatcherIsTranslatedIntoAnInternalGrpcError() {
        var observer = new RecordingObserver<ExportProfilesServiceResponse>();
        new ProfilesServiceAdapter(profiles -> {
            throw new IllegalStateException("profiles boom");
        }, AdmissionControl.unlimited()).export(ProfilesMapper.toProto(TransportFixtures.profilesData()), observer);
        observer.await();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("profiles boom");
    }

    @DisplayName("MetricsServiceAdapter forwards a full-success request")
    @Test
    void metricsAdapterForwardsAFullSuccessRequest() {
        var received = new AtomicReference<ExportMetricsServiceRequest>();
        var observer = new RecordingObserver<ExportMetricsServiceResponse>();
        var sent = TransportFixtures.richMetricsData();
        new MetricsServiceAdapter(request -> {
            received.set(request);
            return CompletableFuture.completedStage(DeliveryResult.accepted(PROV));
        }, AdmissionControl.unlimited()).export(MetricsMapper.toProto(sent), observer);
        observer.await();
        assertThat(received.get()).isEqualTo(MetricsMapper.toProto(sent));
        assertThat(observer.completed).isTrue();
        assertThat(observer.values.getFirst().hasPartialSuccess()).isFalse();
    }

    @DisplayName("LogsServiceAdapter forwards a full-success request")
    @Test
    void logsAdapterForwardsAFullSuccessRequest() {
        var received = new AtomicReference<ExportLogsServiceRequest>();
        var observer = new RecordingObserver<ExportLogsServiceResponse>();
        var sent = TransportFixtures.richLogsData();
        new LogsServiceAdapter(request -> {
            received.set(request);
            return CompletableFuture.completedStage(DeliveryResult.accepted(PROV));
        }, AdmissionControl.unlimited()).export(LogsMapper.toProto(sent), observer);
        observer.await();
        assertThat(received.get()).isEqualTo(LogsMapper.toProto(sent));
        assertThat(observer.completed).isTrue();
        assertThat(observer.values.getFirst().hasPartialSuccess()).isFalse();
    }

    private static final class RecordingObserver<T> implements StreamObserver<T> {
        private final List<T> values = new ArrayList<>();
        private final CompletableFuture<Void> terminal = new CompletableFuture<>();
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            terminal.complete(null);
        }

        @Override
        public void onCompleted() {
            completed = true;
            terminal.complete(null);
        }

        void await() {
            terminal.orTimeout(5, TimeUnit.SECONDS).join();
        }
    }
}
