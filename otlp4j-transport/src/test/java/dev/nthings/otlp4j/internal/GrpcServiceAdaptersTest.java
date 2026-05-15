package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// White-box tests for gRPC service adapters without a server or channel.
///
/// Pins request decoding, [ExportResult] encoding, and error translation at the adapter seam.
class GrpcServiceAdaptersTest {

    @Test
    void traceAdapterDecodesTheRequestAndForwardsItToTheConsumer() {
        var sent = TransportFixtures.richTraceData();
        var received = new AtomicReference<TraceData>();
        var observer = new RecordingObserver<ExportTraceServiceResponse>();

        new TraceServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeTraces(TraceData traces) {
                        received.set(traces);
                        return ExportResult.success();
                    }
                })
                .export(TraceMapper.toProto(sent), observer);

        assertThat(received.get())
                .as("the adapter must hand the consumer the decoded domain object")
                .isEqualTo(sent);
        assertThat(observer.completed).isTrue();
        assertThat(observer.error).isNull();
        assertThat(observer.values).hasSize(1);
        assertThat(observer.values.getFirst().hasPartialSuccess())
                .as("a full success must not carry a partial_success block")
                .isFalse();
    }

    @Test
    void traceAdapterEncodesPartialSuccessOntoTheResponse() {
        var observer = new RecordingObserver<ExportTraceServiceResponse>();

        new TraceServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeTraces(TraceData traces) {
                        return ExportResult.partialSuccess(2, "2 spans bad");
                    }
                })
                .export(TraceMapper.toProto(TransportFixtures.richTraceData()), observer);

        var partial = observer.values.getFirst().getPartialSuccess();
        assertThat(partial.getRejectedSpans()).isEqualTo(2);
        assertThat(partial.getErrorMessage()).isEqualTo("2 spans bad");
        assertThat(observer.completed).isTrue();
    }

    @Test
    void metricsAdapterEncodesPartialSuccessOntoTheResponse() {
        var observer = new RecordingObserver<ExportMetricsServiceResponse>();

        new MetricsServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeMetrics(MetricsData metrics) {
                        return ExportResult.partialSuccess(5, "5 points bad");
                    }
                })
                .export(MetricsMapper.toProto(TransportFixtures.richMetricsData()), observer);

        var partial = observer.values.getFirst().getPartialSuccess();
        assertThat(partial.getRejectedDataPoints()).isEqualTo(5);
        assertThat(partial.getErrorMessage()).isEqualTo("5 points bad");
        assertThat(observer.completed).isTrue();
    }

    @Test
    void logsAdapterEncodesPartialSuccessOntoTheResponse() {
        var observer = new RecordingObserver<ExportLogsServiceResponse>();

        new LogsServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeLogs(LogsData logs) {
                        return ExportResult.partialSuccess(3, "3 records bad");
                    }
                })
                .export(LogsMapper.toProto(TransportFixtures.richLogsData()), observer);

        var partial = observer.values.getFirst().getPartialSuccess();
        assertThat(partial.getRejectedLogRecords()).isEqualTo(3);
        assertThat(partial.getErrorMessage()).isEqualTo("3 records bad");
        assertThat(observer.completed).isTrue();
    }

    @Test
    void profilesAdapterDecodesTheRequestAndForwardsItToTheConsumer() {
        var received = new AtomicReference<ProfilesData>();
        var observer = new RecordingObserver<ExportProfilesServiceResponse>();

        new ProfilesServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeProfiles(ProfilesData profiles) {
                        received.set(profiles);
                        return ExportResult.success();
                    }
                })
                .export(ProfilesMapper.toProto(TransportFixtures.profilesData()), observer);

        // ProfilesMapper is intentionally lossy, so equality with the original cannot hold; the
        // adapter contract is simply that the consumer is fed a decoded, non-null domain object.
        assertThat(received.get()).isNotNull();
        assertThat(observer.completed).isTrue();
        assertThat(observer.values.getFirst().hasPartialSuccess()).isFalse();
    }

    @Test
    void aThrowingConsumerIsTranslatedIntoAnInternalGrpcError() {
        var observer = new RecordingObserver<ExportTraceServiceResponse>();

        new TraceServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeTraces(TraceData traces) {
                        throw new IllegalStateException("handler boom");
                    }
                })
                .export(TraceMapper.toProto(TransportFixtures.richTraceData()), observer);

        assertThat(observer.values).as("a failed call must not stream a response").isEmpty();
        assertThat(observer.completed).isFalse();
        assertThat(observer.error).isNotNull();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("handler boom");
    }

    @Test
    void aThrowingMetricsConsumerIsTranslatedIntoAnInternalGrpcError() {
        var observer = new RecordingObserver<ExportMetricsServiceResponse>();

        new MetricsServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeMetrics(MetricsData metrics) {
                        throw new IllegalStateException("metrics boom");
                    }
                })
                .export(MetricsMapper.toProto(TransportFixtures.richMetricsData()), observer);

        assertThat(observer.values).isEmpty();
        assertThat(observer.completed).isFalse();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("metrics boom");
    }

    @Test
    void aThrowingLogsConsumerIsTranslatedIntoAnInternalGrpcError() {
        var observer = new RecordingObserver<ExportLogsServiceResponse>();

        new LogsServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeLogs(LogsData logs) {
                        throw new IllegalStateException("logs boom");
                    }
                })
                .export(LogsMapper.toProto(TransportFixtures.richLogsData()), observer);

        assertThat(observer.values).isEmpty();
        assertThat(observer.completed).isFalse();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("logs boom");
    }

    @Test
    void aThrowingProfilesConsumerIsTranslatedIntoAnInternalGrpcError() {
        var observer = new RecordingObserver<ExportProfilesServiceResponse>();

        new ProfilesServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeProfiles(ProfilesData profiles) {
                        throw new IllegalStateException("profiles boom");
                    }
                })
                .export(ProfilesMapper.toProto(TransportFixtures.profilesData()), observer);

        assertThat(observer.values).isEmpty();
        assertThat(observer.completed).isFalse();
        var status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).isEqualTo("profiles boom");
    }

    @Test
    void metricsAdapterDecodesAFullSuccessRequest() {
        var received = new AtomicReference<MetricsData>();
        var observer = new RecordingObserver<ExportMetricsServiceResponse>();
        var sent = TransportFixtures.richMetricsData();

        new MetricsServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeMetrics(MetricsData metrics) {
                        received.set(metrics);
                        return ExportResult.success();
                    }
                })
                .export(MetricsMapper.toProto(sent), observer);

        assertThat(received.get()).isEqualTo(sent);
        assertThat(observer.completed).isTrue();
        assertThat(observer.values.getFirst().hasPartialSuccess()).isFalse();
    }

    @Test
    void logsAdapterDecodesAFullSuccessRequest() {
        var received = new AtomicReference<LogsData>();
        var observer = new RecordingObserver<ExportLogsServiceResponse>();
        var sent = TransportFixtures.richLogsData();

        new LogsServiceAdapter(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeLogs(LogsData logs) {
                        received.set(logs);
                        return ExportResult.success();
                    }
                })
                .export(LogsMapper.toProto(sent), observer);

        assertThat(received.get()).isEqualTo(sent);
        assertThat(observer.completed).isTrue();
        assertThat(observer.values.getFirst().hasPartialSuccess()).isFalse();
    }

    /// A minimal [StreamObserver] that records every callback for later assertion — the
    /// hand-rolled stand-in for `io.grpc.testing.StreamRecorder`, which is JUnit-4-oriented and
    /// not worth a dependency for unary calls.
    private static final class RecordingObserver<T> implements StreamObserver<T> {

        private final List<T> values = new ArrayList<>();
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable t) {
            error = t;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }
}
