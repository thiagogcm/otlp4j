package dev.nthings.otlp4j.transport.grpc.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.codec.LogsMapper;
import dev.nthings.otlp4j.codec.MetricsMapper;
import dev.nthings.otlp4j.codec.ProfilesMapper;
import dev.nthings.otlp4j.codec.TraceMapper;
import dev.nthings.otlp4j.testing.TransportFixtures;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
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

    @DisplayName("TraceServiceAdapter decodes the request and forwards it to the dispatcher")
    @Test
    void traceAdapterDecodesTheRequestAndForwardsItToTheDispatcher() {
        var sent = TransportFixtures.richTraceData();
        var received = new AtomicReference<TraceData>();
        var observer = new RecordingObserver<ExportTraceServiceResponse>();

        new TraceServiceAdapter(traces -> {
            received.set(traces);
            return CompletableFuture.completedStage(ConsumeResult.accepted());
        }).export(TraceMapper.toProto(sent), observer);

        observer.await();
        assertThat(received.get()).isEqualTo(sent);
        assertThat(observer.completed).isTrue();
        assertThat(observer.error).isNull();
        assertThat(observer.values.getFirst().hasPartialSuccess()).isFalse();
    }

    @DisplayName("TraceServiceAdapter encodes rejected spans as partial success")
    @Test
    void traceAdapterEncodesPartialSuccessOntoTheResponse() {
        var observer = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                ConsumeResult.partial(2L, "2 spans bad"))).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), observer);
        observer.await();
        var partial = observer.values.getFirst().getPartialSuccess();
        assertThat(partial.getRejectedSpans()).isEqualTo(2);
        assertThat(partial.getErrorMessage()).isEqualTo("2 spans bad");
        assertThat(observer.completed).isTrue();
    }

    @DisplayName("A whole-batch Rejected without a cause maps to retryable UNAVAILABLE, not rejected_spans=0")
    @Test
    void traceAdapterMapsRejectedToUnavailable() {
        var observer = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                ConsumeResult.rejected("queue full"))).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), observer);
        observer.await();
        assertThat(observer.values).isEmpty();
        assertThat(observer.completed).isFalse();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(status.getDescription()).isEqualTo("queue full");
    }

    @DisplayName("A whole-batch Rejected carrying a cause maps to INTERNAL")
    @Test
    void rejectedWithCauseMapsToInternal() {
        var observer = new RecordingObserver<ExportLogsServiceResponse>();
        new LogsServiceAdapter(logs -> CompletableFuture.completedStage(
                ConsumeResult.rejected("stage threw", new IllegalStateException("boom")))).export(
                        LogsMapper.toProto(TransportFixtures.richLogsData()), observer);
        observer.await();
        assertThat(observer.values).isEmpty();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("stage threw");
    }

    /// Pins the #9d contract: the cause is the ONLY thing that distinguishes a transient rejection
    /// (UNAVAILABLE, retried) from a permanent one (INTERNAL, not retried). A deterministic dropper
    /// must attach a cause or its batch is retried until the client's budget is spent.
    @DisplayName("The cause alone flips a no-cause UNAVAILABLE rejection to a non-retryable INTERNAL")
    @Test
    void rejectionCauseDecidesRetryability() {
        var noCause = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                ConsumeResult.rejected("dropped by policy"))).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), noCause);
        noCause.await();
        assertThat(Status.fromThrowable(noCause.error).getCode()).isEqualTo(Status.Code.UNAVAILABLE);

        var withCause = new RecordingObserver<ExportTraceServiceResponse>();
        new TraceServiceAdapter(traces -> CompletableFuture.completedStage(
                ConsumeResult.rejected("dropped by policy", new IllegalStateException("disallowed")))).export(
                        TraceMapper.toProto(TransportFixtures.richTraceData()), withCause);
        withCause.await();
        assertThat(Status.fromThrowable(withCause.error).getCode()).isEqualTo(Status.Code.INTERNAL);
    }

    @DisplayName("MetricsServiceAdapter encodes rejected data points as partial success")
    @Test
    void metricsAdapterEncodesPartialSuccessOntoTheResponse() {
        var observer = new RecordingObserver<ExportMetricsServiceResponse>();
        new MetricsServiceAdapter(metrics -> CompletableFuture.completedStage(
                ConsumeResult.partial(5L, "5 points bad"))).export(
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
                ConsumeResult.partial(3L, "3 records bad"))).export(
                        LogsMapper.toProto(TransportFixtures.richLogsData()), observer);
        observer.await();
        var partial = observer.values.getFirst().getPartialSuccess();
        assertThat(partial.getRejectedLogRecords()).isEqualTo(3);
        assertThat(partial.getErrorMessage()).isEqualTo("3 records bad");
        assertThat(observer.completed).isTrue();
    }

    @DisplayName("ProfilesServiceAdapter decodes the request and forwards it to the dispatcher")
    @Test
    void profilesAdapterDecodesTheRequestAndForwardsItToTheDispatcher() {
        var received = new AtomicReference<ProfilesData>();
        var observer = new RecordingObserver<ExportProfilesServiceResponse>();
        new ProfilesServiceAdapter(profiles -> {
            received.set(profiles);
            return CompletableFuture.completedStage(ConsumeResult.accepted());
        }).export(ProfilesMapper.toProto(TransportFixtures.profilesData()), observer);
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
        }).export(TraceMapper.toProto(TransportFixtures.richTraceData()), observer);
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
        }).export(MetricsMapper.toProto(TransportFixtures.richMetricsData()), observer);
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
        }).export(LogsMapper.toProto(TransportFixtures.richLogsData()), observer);
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
        }).export(ProfilesMapper.toProto(TransportFixtures.profilesData()), observer);
        observer.await();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("profiles boom");
    }

    @DisplayName("MetricsServiceAdapter decodes a full-success request")
    @Test
    void metricsAdapterDecodesAFullSuccessRequest() {
        var received = new AtomicReference<MetricsData>();
        var observer = new RecordingObserver<ExportMetricsServiceResponse>();
        var sent = TransportFixtures.richMetricsData();
        new MetricsServiceAdapter(metrics -> {
            received.set(metrics);
            return CompletableFuture.completedStage(ConsumeResult.accepted());
        }).export(MetricsMapper.toProto(sent), observer);
        observer.await();
        assertThat(received.get()).isEqualTo(sent);
        assertThat(observer.completed).isTrue();
        assertThat(observer.values.getFirst().hasPartialSuccess()).isFalse();
    }

    @DisplayName("LogsServiceAdapter decodes a full-success request")
    @Test
    void logsAdapterDecodesAFullSuccessRequest() {
        var received = new AtomicReference<LogsData>();
        var observer = new RecordingObserver<ExportLogsServiceResponse>();
        var sent = TransportFixtures.richLogsData();
        new LogsServiceAdapter(logs -> {
            received.set(logs);
            return CompletableFuture.completedStage(ConsumeResult.accepted());
        }).export(LogsMapper.toProto(sent), observer);
        observer.await();
        assertThat(received.get()).isEqualTo(sent);
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
