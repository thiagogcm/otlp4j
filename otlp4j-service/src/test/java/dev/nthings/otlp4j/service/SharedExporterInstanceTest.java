package dev.nthings.otlp4j.service;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.component.SignalSupport;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/// Proves the exporter shared-component guarantee: one exporter [ComponentId] referenced as the
/// terminal exporter of two pipelines of different signals is backed by exactly one underlying
/// instance, its per-signal facets sharing it, mirroring how a receiver's single instance serves every
/// signal it feeds. A factory shares its underlying client through the build-scoped
/// [dev.nthings.otlp4j.core.component.SharedComponents] cache handed to it on every [CreateContext],
/// so the second and later facets of the same id reuse the first facet's instance rather than each
/// opening its own.
class SharedExporterInstanceTest {

    private static final ComponentId R = ComponentId.parse("otlp");
    private static final ComponentId E = ComponentId.parse("otlpexp");

    @Test
    void oneExporterIdAcrossTwoSignalPipelinesBuildsOneUnderlyingInstance() {
        var clientBuilds = new AtomicInteger();
        var facetCreates = new AtomicInteger();
        var exporterFactory = new SharedCountingExporterFactory("otlpexp", clientBuilds, facetCreates);
        var recv = new Fakes.ReceiverFactoryImpl("otlp", new Fakes.Log(), null);

        var gw = Gateway.builder();
        gw.receiver(R, recv, Fakes.Config.ok());
        gw.exporter(E, exporterFactory, Fakes.Config.ok());
        gw.pipeline(PipelineId.of(TRACES), new PipelineConfig(List.of(R), List.of(), List.of(E)));
        gw.pipeline(PipelineId.of(METRICS), new PipelineConfig(List.of(R), List.of(), List.of(E)));
        gw.build();

        // One per-signal facet is created for each of the two pipeline signals ...
        assertThat(facetCreates.get()).isEqualTo(2);
        // ... but the shared underlying instance is built exactly once for the single exporter id.
        assertThat(clientBuilds.get()).isEqualTo(1);
    }

    /// An exporter factory that shares one underlying client per id through the create context's
    /// shared-component cache, counting both underlying builds and per-signal facet creations so the
    /// test can assert one instance backs the two facets.
    private static final class SharedCountingExporterFactory
            implements ExporterFactory<Fakes.Config>, SignalSupport {

        private final String type;
        private final AtomicInteger clientBuilds;
        private final AtomicInteger facetCreates;

        SharedCountingExporterFactory(String type, AtomicInteger clientBuilds, AtomicInteger facetCreates) {
            this.type = type;
            this.clientBuilds = clientBuilds;
            this.facetCreates = facetCreates;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.EXPORTER;
        }

        @Override
        public Fakes.Config defaultConfig() {
            return Fakes.Config.ok();
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public boolean supportsSignal(SignalDescriptor<?> signal) {
            return true;
        }

        @Override
        public <T extends TelemetryPayload> Exporter<T> create(
                CreateContext ctx, Fakes.Config config, SignalDescriptor<T> signal) {
            // Build the underlying client once per id; every later per-signal facet of the same id
            // reuses it. This is the shared-component guarantee the graph builder provides.
            ctx.sharedComponents().computeIfAbsent(ctx.id(), () -> {
                clientBuilds.incrementAndGet();
                return new Object();
            });
            facetCreates.incrementAndGet();
            return new CountingExporter<>(ctx.id(), signal);
        }
    }

    /// A minimal per-signal exporter facet with a no-op lifecycle; its identity is all the test reads.
    private static final class CountingExporter<T extends TelemetryPayload> implements Exporter<T> {

        private final ComponentId id;
        private final SignalDescriptor<T> signal;

        CountingExporter(ComponentId id, SignalDescriptor<T> signal) {
            this.id = id;
            this.signal = signal;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, T data) {
            return CompletableFuture.completedFuture(
                    DeliveryResult.accepted(new Provenance(id, ComponentKind.EXPORTER, signal)));
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> forceFlush(FlushContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
