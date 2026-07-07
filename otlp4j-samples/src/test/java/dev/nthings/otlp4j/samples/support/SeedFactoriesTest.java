package dev.nthings.otlp4j.samples.support;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.codec.DomainRequestPayload;
import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// The demo seed factories exercised through a real service graph runtime: a function processor
/// transforms and drops batches on the way to an exporter, and a count connector derives a metric
/// from a traces batch and emits it into a metrics pipeline.
@DisplayName("Demo seed factories")
class SeedFactoriesTest {

    private static final ComponentId RECV = ComponentId.parse("in");
    private static final ComponentId PROC = ComponentId.parse("fn");
    private static final ComponentId EXP = ComponentId.parse("out");
    private static final ComponentId CONN = ComponentId.parse("spancount");
    private static final ComponentId METRICS_EXP = ComponentId.parse("metricsout");

    private static Span sampleSpan() {
        return Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name("op")
                .kind(Span.Kind.SERVER)
                .startEpochNanos(1L)
                .endEpochNanos(2L)
                .build();
    }

    private static TracesData traces(int spans) {
        var list = new java.util.ArrayList<Span>();
        for (var i = 0; i < spans; i++) {
            list.add(sampleSpan());
        }
        return TracesData.of(Resource.EMPTY, InstrumentationScope.EMPTY, list);
    }

    @SuppressWarnings("unchecked")
    private static SignalConsumer<TelemetryPayload> tracesConsumer(SignalConsumers bundle) {
        return (SignalConsumer<TelemetryPayload>) bundle.consumer((SignalDescriptor<TelemetryPayload>) (SignalDescriptor<?>) TRACES);
    }

    @DisplayName("a function processor transforms the batch and forwards it to the exporter")
    @Test
    void functionProcessorTransformsAndForwards() {
        var received = new CopyOnWriteArrayList<TelemetryPayload>();
        var recv = new CapturingReceiverFactory();
        var enrich = new FunctionProcessorFactory<>(
                "fn", TRACES, (TracesData t) -> traces(2));
        var exp = new RecordingExporterFactory(received);

        var runtime = Gateway.builder()
                .receiver(RECV, recv, EmptyConfig.INSTANCE)
                .processor(PROC, enrich, FunctionProcessorConfig.defaults())
                .exporter(EXP, exp, EmptyConfig.INSTANCE)
                .pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(RECV), List.of(PROC), List.of(EXP)))
                .build();
        runtime.start();

        var result = deliver(recv, Payloads.domain(TRACES, traces(1)));

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received).hasSize(1);
        assertThat(received.get(0).itemCount()).isEqualTo(2);
        runtime.close();
    }

    @DisplayName("a function processor that drops a batch forwards nothing and still accepts")
    @Test
    void functionProcessorDropsBatch() {
        var received = new CopyOnWriteArrayList<TelemetryPayload>();
        var recv = new CapturingReceiverFactory();
        var drop = new FunctionProcessorFactory<TracesData>("fn", TRACES, t -> null);
        var exp = new RecordingExporterFactory(received);

        var runtime = Gateway.builder()
                .receiver(RECV, recv, EmptyConfig.INSTANCE)
                .processor(PROC, drop, FunctionProcessorConfig.defaults())
                .exporter(EXP, exp, EmptyConfig.INSTANCE)
                .pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(RECV), List.of(PROC), List.of(EXP)))
                .build();
        runtime.start();

        var result = deliver(recv, Payloads.domain(TRACES, traces(1)));

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received).isEmpty();
        runtime.close();
    }

    @DisplayName("a count connector derives a metric from traces and emits it into the metrics pipeline")
    @Test
    void countConnectorDerivesMetric() {
        var received = new CopyOnWriteArrayList<TelemetryPayload>();
        var recv = new CapturingReceiverFactory();
        var connector = new CountConnectorFactory();
        var metricsExp = new RecordingExporterFactory(received);

        var runtime = Gateway.builder()
                .receiver(RECV, recv, EmptyConfig.INSTANCE)
                .connector(CONN, connector, CountConnectorConfig.defaults())
                .exporter(METRICS_EXP, metricsExp, EmptyConfig.INSTANCE)
                .pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(RECV), List.of(), List.of(CONN)))
                .pipeline(PipelineId.of(METRICS), new PipelineConfig(List.of(CONN), List.of(), List.of(METRICS_EXP)))
                .build();
        runtime.start();

        var result = deliver(recv, Payloads.domain(TRACES, traces(3)));

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received).hasSize(1);
        assertThat(received.get(0).signal()).isEqualTo(METRICS);
        var metrics = (MetricsData) ((DomainRequestPayload<?, ?, ?>) received.get(0)).batch();
        assertThat(metrics.resourceMetrics()).isNotEmpty();
        runtime.close();
    }

    private static DeliveryResult deliver(CapturingReceiverFactory recv, TelemetryPayload payload) {
        return tracesConsumer(recv.consumers()).consume(new TestContext(), payload).toCompletableFuture().join();
    }

    // ---- test fixtures ---------------------------------------------------------------------------

    private record EmptyConfig() implements ComponentConfig {
        static final EmptyConfig INSTANCE = new EmptyConfig();
    }

    private static final class TestContext implements ConsumeContext {
        private final Deadline deadline = Deadline.after(Duration.ofSeconds(5), Clock.systemUTC());

        @Override
        public ClientMetadata metadata() {
            return ClientMetadata.empty();
        }

        @Override
        public Deadline deadline() {
            return deadline;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void onCancel(Runnable callback) {
            // never cancelled
        }
    }

    private static final class CapturingReceiverFactory implements ReceiverFactory<EmptyConfig> {
        private @org.jspecify.annotations.Nullable SignalConsumers consumers;

        SignalConsumers consumers() {
            var c = consumers;
            if (c == null) {
                throw new IllegalStateException("receiver not created yet");
            }
            return c;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.RECEIVER;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.DEVELOPMENT;
        }

        @Override
        public EmptyConfig defaultConfig() {
            return EmptyConfig.INSTANCE;
        }

        @Override
        public String type() {
            return "in";
        }

        @Override
        public Receiver create(CreateContext ctx, EmptyConfig config, SignalConsumers consumers) {
            this.consumers = consumers;
            return new NoopReceiver();
        }
    }

    private static final class NoopReceiver implements Receiver {
        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class RecordingExporterFactory implements ExporterFactory<EmptyConfig> {
        private final List<TelemetryPayload> received;

        RecordingExporterFactory(List<TelemetryPayload> received) {
            this.received = received;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.EXPORTER;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.DEVELOPMENT;
        }

        @Override
        public EmptyConfig defaultConfig() {
            return EmptyConfig.INSTANCE;
        }

        @Override
        public String type() {
            return "out";
        }

        @Override
        public <T extends TelemetryPayload> Exporter<T> create(
                CreateContext ctx, EmptyConfig config, SignalDescriptor<T> signal) {
            return new RecordingExporter<>(ctx.id(), signal, received);
        }
    }

    private static final class RecordingExporter<T extends TelemetryPayload> implements Exporter<T> {
        private final ComponentId id;
        private final SignalDescriptor<T> signal;
        private final List<TelemetryPayload> received;

        RecordingExporter(ComponentId id, SignalDescriptor<T> signal, List<TelemetryPayload> received) {
            this.id = id;
            this.signal = signal;
            this.received = received;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, T data) {
            received.add(data);
            return DeliveryResult.acceptedStage(new Provenance(id, ComponentKind.EXPORTER, signal));
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
