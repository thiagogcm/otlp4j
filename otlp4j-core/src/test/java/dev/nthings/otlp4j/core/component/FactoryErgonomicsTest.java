package dev.nthings.otlp4j.core.component;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests the typed processor and exporter factory ergonomics.
class FactoryErgonomicsTest {

    private static final ComponentId ID = ComponentId.parse("fn");

    @Test
    @DisplayName("map rejects a wrong-signal creation")
    void processorRejectsWrongSignal() {
        var factory = ProcessorFactory.map("enrich", TRACES, value -> value);
        assertThatThrownBy(() -> factory.create(
                        new CreateContext(ID, base()), ComponentConfig.NONE, METRICS, sink(METRICS, new AtomicReference<>())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a map processor forwards the transformed typed batch")
    void mapForwards() {
        var seen = new AtomicReference<Payload<TracesData>>();
        var factory = ProcessorFactory.map("enrich", TRACES, value -> traces(2));
        var processor = factory.create(
                new CreateContext(ID, base()), ComponentConfig.NONE, TRACES, sink(TRACES, seen));

        var result = processor.consume(ConsumeContext.defaults(), Payload.of(TRACES, traces(1)))
                .toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(seen.get()).isNotNull();
        assertThat(seen.get().data().spanCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("a filter processor drops a non-matching batch")
    void filterDrops() {
        var seen = new AtomicReference<Payload<TracesData>>();
        var factory = ProcessorFactory.filter("keep", TRACES, value -> false);
        var processor = factory.create(
                new CreateContext(ID, base()), ComponentConfig.NONE, TRACES, sink(TRACES, seen));

        var result = processor.consume(ConsumeContext.defaults(), Payload.of(TRACES, traces(1)))
                .toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(seen.get()).isNull();
    }

    @Test
    @DisplayName("ExporterFactory.of delegates typed payloads with facet provenance")
    void exporterDelegates() {
        var seenProvenance = new AtomicReference<Provenance>();
        var seenData = new AtomicReference<Payload<?>>();
        var factory = ExporterFactory.of("capture", (ctx, data, provenance) -> {
            seenProvenance.set(provenance);
            seenData.set(data);
            return DeliveryResult.acceptedStage(provenance);
        });
        var exporter = factory.create(new CreateContext(ID, base()), ComponentConfig.NONE, TRACES);
        var payload = Payload.of(TRACES, traces(1));
        var result = exporter.consume(ConsumeContext.defaults(), payload).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(seenData.get()).isSameAs(payload);
        assertThat(seenProvenance.get()).isEqualTo(new Provenance(ID, ComponentKind.EXPORTER, TRACES));
        assertThat(exporter.start(base()).toCompletableFuture()).isCompleted();
        assertThat(exporter.shutdown(new ShutdownContext(Duration.ZERO)).toCompletableFuture()).isCompleted();
    }

    @Test
    @DisplayName("ExporterFactory.of rejects a null terminal")
    void exporterRejectsNull() {
        assertThatThrownBy(() -> ExporterFactory.of("capture", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ComponentContext base() {
        return new ComponentContext(dev.nthings.otlp4j.core.telemetry.GatewayMetrics.noop(), Clock.systemUTC());
    }

    private static <M> SignalConsumer<M> sink(
            SignalDescriptor<M> signal,
            AtomicReference<? super Payload<M>> seen) {
        return new SignalConsumer<>() {
            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<M> data) {
                seen.set(data);
                return DeliveryResult.acceptedStage(new Provenance(ID, ComponentKind.EXPORTER, signal));
            }
        };
    }

    private static TracesData traces(int spans) {
        var list = new ArrayList<Span>();
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
}
