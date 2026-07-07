package dev.nthings.otlp4j.service;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Extensions;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Proves the Lot 3 change: the graph builds the extensions before the data path and threads an
/// [Extensions] view into every data-path create context, so a receiver, processor, exporter, or
/// connector factory can resolve a referenced extension by id at create time. The topology is
/// `otlp -> capture/1 -> otlpexp` plus a `health` extension; the capturing processor factory records
/// the [Extensions] view it is handed so the test can interrogate it.
class ExtensionResolutionTest {

    private static final ComponentId R = ComponentId.parse("otlp");
    private static final ComponentId P = ComponentId.parse("capture/1");
    private static final ComponentId E = ComponentId.parse("otlpexp");
    private static final ComponentId X = ComponentId.parse("health");
    private static final ComponentId ABSENT = ComponentId.parse("nosuch");
    private static final PipelineId TRACES_PIPE = PipelineId.of(TRACES);

    private final Fakes.Log log = new Fakes.Log();

    private final AtomicReference<Extensions> captured = new AtomicReference<>();

    private GatewayRuntime buildChain() {
        var recv = new Fakes.ReceiverFactoryImpl("otlp", log, null);
        var proc = new CapturingProcessorFactory("capture", log, captured);
        var exp = new Fakes.ExporterFactoryImpl("otlpexp", log, null, null);
        var ext = new Fakes.ExtensionFactoryImpl("health", log);
        return Gateway.builder()
                .receiver(R, recv, Fakes.Config.ok())
                .processor(P, proc, Fakes.Config.ok())
                .exporter(E, exp, Fakes.Config.ok())
                .extension(X, ext, Fakes.Config.ok())
                .service(new ServiceOptions(Duration.ofSeconds(5), Duration.ofSeconds(5), List.of(X)))
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(P), List.of(E)))
                .build();
    }

    @Test
    void dataPathFactoryResolvesBuiltExtensionById() {
        var runtime = buildChain();

        var view = captured.get();
        assertThat(view).as("a data-path factory is handed an Extensions view").isNotNull();
        var resolved = view.get(X);
        assertThat(resolved).as("the referenced extension resolves from the data path").isPresent();
        assertThat(resolved.orElseThrow())
                .as("it resolves to the same live extension the runtime exposes")
                .isSameAs(runtime.component(ComponentKind.EXTENSION, X).orElseThrow());
    }

    @Test
    void resolvingAnUnbuiltIdIsEmpty() {
        buildChain();

        assertThat(captured.get().get(ABSENT)).isEmpty();
    }

    @Test
    void extensionsAreBuiltBeforeTheDataPath() {
        buildChain();

        // The extension is created before any data-path component, which is what makes it resolvable
        // at data-path create time. Start and shutdown order is unchanged and covered by
        // LifecycleOrderTest.
        var creates = log.events().stream().filter(e -> e.startsWith("create:")).toList();
        assertThat(creates).first().isEqualTo("create:health");
    }

    /// A processor factory that records the [Extensions] view its create context carries, then delegates
    /// to a plain pass-through processor so the graph builds and validates normally.
    private static final class CapturingProcessorFactory implements ProcessorFactory<Fakes.Config> {
        private final String type;
        private final Fakes.Log log;
        private final AtomicReference<Extensions> sink;

        CapturingProcessorFactory(String type, Fakes.Log log, AtomicReference<Extensions> sink) {
            this.type = type;
            this.log = log;
            this.sink = sink;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.PROCESSOR;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.BETA;
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
        public <T extends TelemetryPayload> Processor<T> create(
                CreateContext ctx, Fakes.Config config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
            sink.set(ctx.extensions());
            log.record("create:" + ctx.id());
            return new Fakes.ProcessorImpl<>(ctx.id(), log, config, next, false);
        }
    }
}
