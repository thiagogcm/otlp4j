package dev.nthings.otlp4j.core.component;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import java.io.OutputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the component-SPI ergonomics: the [ComponentConfig#NONE] singleton, the functional
/// [ProcessorFactory#of] and [ExporterFactory#of] factories, and the shared unchecked-cast helpers.
class FactoryErgonomicsTest {

    private static final ComponentId ID = ComponentId.parse("fn");

    /// A [PayloadFactory] that wraps a batch in a [FakePayload], standing in for the codec's real one.
    private static final PayloadFactory PAYLOADS = new PayloadFactory() {
        @Override
        public <T> TelemetryPayload domain(SignalDescriptor<T> signal, T batch) {
            return new FakePayload(signal, batch);
        }
    };

    @Test
    @DisplayName("ComponentConfig.NONE is the shared no-op configuration")
    void noneConfig() {
        assertThat(ComponentConfig.NONE).isInstanceOf(ComponentConfig.None.class);
        assertThat(ComponentConfig.NONE).isEqualTo(new ComponentConfig.None());
    }

    @Test
    @DisplayName("ProcessorFactory.of declares processor metadata and the no-op config")
    void processorMetadata() {
        var factory = ProcessorFactory.of("enrich", TRACES, t -> t, PAYLOADS);
        assertThat(factory.kind()).isEqualTo(ComponentKind.PROCESSOR);
        assertThat(factory.type()).isEqualTo("enrich");
        assertThat(factory.defaultConfig()).isEqualTo(ComponentConfig.NONE);
    }

    @Test
    @DisplayName("ProcessorFactory.of rejects a null argument and a wrong-signal creation")
    void processorRejectsBadInput() {
        assertThatThrownBy(() -> ProcessorFactory.of("enrich", TRACES, null, PAYLOADS))
                .isInstanceOf(IllegalArgumentException.class);
        var factory = ProcessorFactory.of("enrich", TRACES, t -> t, PAYLOADS);
        var ctx = new CreateContext(ID, base());
        assertThatThrownBy(() -> factory.create(ctx, ComponentConfig.NONE, cast(METRICS), sink(new AtomicReference<>())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a transform processor forwards the transformed batch downstream as a domain payload")
    void processorForwards() {
        var seen = new AtomicReference<TelemetryPayload>();
        var factory = ProcessorFactory.of("enrich", TRACES, t -> traces(2), PAYLOADS);
        var processor = factory.create(new CreateContext(ID, base()), ComponentConfig.NONE, cast(TRACES), sink(seen));
        assertThat(processor.capabilities()).isEqualTo(ConsumerCapabilities.nonMutating());

        var result = processor.consume(new TestContext(), new FakePayload(TRACES, traces(1)))
                .toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(seen.get()).isNotNull();
        assertThat(((TracesData) seen.get().view(PayloadView.DOMAIN_IMMUTABLE)).spanCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("a transform processor that returns null drops the batch and forwards nothing")
    void processorDrops() {
        var seen = new AtomicReference<TelemetryPayload>();
        var factory = ProcessorFactory.of("drop", TRACES, (TracesData t) -> null, PAYLOADS);
        var processor = factory.create(new CreateContext(ID, base()), ComponentConfig.NONE, cast(TRACES), sink(seen));

        var result = processor.consume(new TestContext(), new FakePayload(TRACES, traces(1)))
                .toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(seen.get()).isNull();
    }

    @Test
    @DisplayName("a transform processor starts and shuts down cleanly")
    void processorLifecycle() {
        var factory = ProcessorFactory.of("enrich", TRACES, t -> t, PAYLOADS);
        var processor = factory.create(new CreateContext(ID, base()), ComponentConfig.NONE, cast(TRACES),
                sink(new AtomicReference<>()));
        assertThat(processor.start(base()).toCompletableFuture()).isCompleted();
        assertThat(processor.shutdown(new ShutdownContext(Duration.ZERO)).toCompletableFuture()).isCompleted();
    }

    @Test
    @DisplayName("ExporterFactory.of delegates each consume to the terminal with the facet provenance")
    void exporterDelegates() {
        var seenProvenance = new AtomicReference<Provenance>();
        var seenData = new AtomicReference<TelemetryPayload>();
        var factory = ExporterFactory.of("capture", (ctx, data, provenance) -> {
            seenProvenance.set(provenance);
            seenData.set(data);
            return DeliveryResult.acceptedStage(provenance);
        });
        assertThat(factory.kind()).isEqualTo(ComponentKind.EXPORTER);
        assertThat(factory.type()).isEqualTo("capture");
        assertThat(factory.defaultConfig()).isEqualTo(ComponentConfig.NONE);

        var exporter = factory.create(new CreateContext(ID, base()), ComponentConfig.NONE, cast(TRACES));
        assertThat(exporter.capabilities()).isEqualTo(ConsumerCapabilities.nonMutating());
        var payload = new FakePayload(TRACES, traces(1));
        var result = exporter.consume(new TestContext(), payload).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(seenData.get()).isSameAs(payload);
        assertThat(seenProvenance.get()).isEqualTo(new Provenance(ID, ComponentKind.EXPORTER, TRACES));
        assertThat(exporter.start(base()).toCompletableFuture()).isCompleted();
        assertThat(exporter.shutdown(new ShutdownContext(Duration.ZERO)).toCompletableFuture()).isCompleted();
    }

    @Test
    @DisplayName("ExporterFactory.of rejects a null terminal")
    void exporterRejectsNull() {
        assertThatThrownBy(() -> ExporterFactory.of("capture", null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("the cast helpers reinterpret without altering the instance")
    void castHelpers() {
        SignalConsumer<TelemetryPayload> consumer = sink(new AtomicReference<>());
        assertThat(ProcessorFactory.asPayloadConsumer(consumer)).isSameAs(consumer);
        assertThat(ProcessorFactory.<TracesData>asDomainDescriptor(TRACES)).isSameAs(TRACES);
        Processor<TelemetryPayload> processor = ProcessorFactory.of("enrich", TRACES, t -> t, PAYLOADS)
                .create(new CreateContext(ID, base()), ComponentConfig.NONE, cast(TRACES), sink(new AtomicReference<>()));
        assertThat(ProcessorFactory.<TelemetryPayload>asTypedProcessor(processor)).isSameAs(processor);
    }

    // ---- fixtures --------------------------------------------------------------------------------

    private static ComponentContext base() {
        return new ComponentContext(dev.nthings.otlp4j.core.telemetry.GatewayMetrics.noop(), Clock.systemUTC());
    }

    @SuppressWarnings("unchecked")
    private static SignalDescriptor<TelemetryPayload> cast(SignalDescriptor<?> signal) {
        return (SignalDescriptor<TelemetryPayload>) signal;
    }

    private static SignalConsumer<TelemetryPayload> sink(AtomicReference<TelemetryPayload> seen) {
        return new SignalConsumer<>() {
            @Override
            public ConsumerCapabilities capabilities() {
                return ConsumerCapabilities.nonMutating();
            }

            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
                seen.set(data);
                return DeliveryResult.acceptedStage(new Provenance(ID, ComponentKind.EXPORTER, TRACES));
            }
        };
    }

    private static TracesData traces(int spans) {
        var list = new java.util.ArrayList<Span>();
        for (var i = 0; i < spans; i++) {
            list.add(Span.builder()
                    .traceId("0102030405060708090a0b0c0d0e0f10")
                    .spanId("0102030405060708")
                    .name("op")
                    .kind(Span.Kind.SERVER)
                    .startEpochNanos(1L)
                    .endEpochNanos(2L)
                    .build());
        }
        return TracesData.of(Resource.EMPTY, InstrumentationScope.EMPTY, list);
    }

    /// A minimal domain-backed payload: it answers the domain view with its batch, which is all the
    /// functional factories read.
    private record FakePayload(SignalDescriptor<?> signal, Object batch) implements TelemetryPayload {
        @Override
        public long itemCount() {
            return 0;
        }

        @Override
        public long estimatedSerializedSize() {
            return 0;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V view(PayloadView<V> view) {
            if (view == PayloadView.DOMAIN_IMMUTABLE) {
                return (V) batch;
            }
            throw new IllegalArgumentException("unsupported view: " + view);
        }

        @Override
        public TelemetryPayload copyForMutation() {
            return this;
        }

        @Override
        public void writeTo(OutputStream out) {
            throw new UnsupportedOperationException();
        }
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
}
