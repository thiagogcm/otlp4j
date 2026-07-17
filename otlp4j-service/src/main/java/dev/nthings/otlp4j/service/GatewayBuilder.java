package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ConnectorFactory;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.service.config.ConnectorEntry;
import dev.nthings.otlp4j.service.config.ExporterEntry;
import dev.nthings.otlp4j.service.config.GatewayConfig;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.config.ProcessorEntry;
import dev.nthings.otlp4j.service.config.ReceiverEntry;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/// The one fluent builder [Gateway#builder] returns. It registers components by id and collects
/// pipelines, then compiles to a [GatewayConfig] the [GatewayCompiler] materializes; there is no
/// second construction path (the declarative alternative builds a [GatewayConfig] directly and passes
/// it to [Gateway#compile]).
///
/// Each registration returns a typed handle carrying the component's id: `receiver(...)` a
/// [ReceiverHandle], `processor(...)` a [ProcessorHandle], `exporter(...)` an [ExporterHandle], and
/// `connector(...)` a [ConnectorHandle]. Feeding those handles to the typed
/// [#pipeline(PipelineId, Consumer)] block wires a pipeline with the component kinds checked at compile
/// time, so a receiver used as an exporter, or an unregistered id, is a compile error rather than one
/// of the runtime `UNKNOWN_*` diagnostics. The raw [#pipeline(PipelineId, PipelineConfig)] overload
/// keeps the stringly-typed path for a programmatic or declarative caller, and it keeps the runtime
/// diagnostics.
public final class GatewayBuilder {

    private final Map<ComponentId, ReceiverEntry<?>> receivers = new LinkedHashMap<>();
    private final Map<ComponentId, ProcessorEntry<?>> processors = new LinkedHashMap<>();
    private final Map<ComponentId, ExporterEntry<?>> exporters = new LinkedHashMap<>();
    private final Map<ComponentId, ConnectorEntry<?>> connectors = new LinkedHashMap<>();
    private final Map<PipelineId, PipelineConfig> pipelines = new LinkedHashMap<>();

    private ServiceOptions service = ServiceOptions.defaults();

    GatewayBuilder() {}

    /// Registers a receiver under `id`, created from `config`.
    ///
    /// @param id      the component id
    /// @param factory the receiver factory
    /// @param config  the receiver config
    /// @param <C>     the config type
    /// @return a handle to the registered receiver
    public <C extends ComponentConfig> ReceiverHandle receiver(
            ComponentId id, ReceiverFactory<C> factory, C config) {
        receivers.put(id, new ReceiverEntry<>(factory, config));
        return new ReceiverHandle(id);
    }

    /// Registers a receiver under `id`, created from the factory's default config.
    ///
    /// @param id      the component id
    /// @param factory the receiver factory
    /// @param <C>     the config type
    /// @return a handle to the registered receiver
    public <C extends ComponentConfig> ReceiverHandle receiver(ComponentId id, ReceiverFactory<C> factory) {
        return receiver(id, factory, factory.defaultConfig());
    }

    /// Registers a processor under `id`, created from `config`.
    ///
    /// @param id      the component id
    /// @param factory the processor factory
    /// @param config  the processor config
    /// @param <C>     the config type
    /// @return a handle to the registered processor
    public <C extends ComponentConfig> ProcessorHandle processor(
            ComponentId id, ProcessorFactory<C> factory, C config) {
        processors.put(id, new ProcessorEntry<>(factory, config));
        return new ProcessorHandle(id);
    }

    /// Registers a processor under `id`, created from the factory's default config.
    ///
    /// @param id      the component id
    /// @param factory the processor factory
    /// @param <C>     the config type
    /// @return a handle to the registered processor
    public <C extends ComponentConfig> ProcessorHandle processor(ComponentId id, ProcessorFactory<C> factory) {
        return processor(id, factory, factory.defaultConfig());
    }

    /// Registers an exporter under `id`, created from `config`.
    ///
    /// @param id      the component id
    /// @param factory the exporter factory
    /// @param config  the exporter config
    /// @param <C>     the config type
    /// @return a handle to the registered exporter
    public <C extends ComponentConfig> ExporterHandle exporter(
            ComponentId id, ExporterFactory<C> factory, C config) {
        exporters.put(id, new ExporterEntry<>(factory, config));
        return new ExporterHandle(id);
    }

    /// Registers an exporter under `id`, created from the factory's default config.
    ///
    /// @param id      the component id
    /// @param factory the exporter factory
    /// @param <C>     the config type
    /// @return a handle to the registered exporter
    public <C extends ComponentConfig> ExporterHandle exporter(ComponentId id, ExporterFactory<C> factory) {
        return exporter(id, factory, factory.defaultConfig());
    }

    /// Registers a connector under `id`, created from `config`.
    ///
    /// @param id      the component id
    /// @param factory the connector factory
    /// @param config  the connector config
    /// @param <C>     the config type
    /// @return a handle to the registered connector
    public <C extends ComponentConfig> ConnectorHandle connector(
            ComponentId id, ConnectorFactory<C> factory, C config) {
        connectors.put(id, new ConnectorEntry<>(factory, config));
        return new ConnectorHandle(id);
    }

    /// Registers a connector under `id`, created from the factory's default config.
    ///
    /// @param id      the component id
    /// @param factory the connector factory
    /// @param <C>     the config type
    /// @return a handle to the registered connector
    public <C extends ComponentConfig> ConnectorHandle connector(ComponentId id, ConnectorFactory<C> factory) {
        return connector(id, factory, factory.defaultConfig());
    }

    /// Adds the pipeline `id` from a typed handle block, `p -> p.from(in).via(enrich).to(out)`. The
    /// handles are the ones the registrations returned, so a wrong-kind or unregistered reference is a
    /// compile error.
    ///
    /// @param id   the pipeline id
    /// @param spec the pipeline wiring block
    /// @return this builder
    public GatewayBuilder pipeline(PipelineId id, Consumer<PipelineBuilder> spec) {
        var pb = new PipelineBuilder();
        spec.accept(pb);
        pipelines.put(id, pb.toConfig());
        return this;
    }

    /// Adds the pipeline `id` from a raw [PipelineConfig], the stringly-typed path a programmatic or
    /// declarative caller uses; a wrong-kind or unregistered id is a runtime validation diagnostic.
    ///
    /// @param id   the pipeline id
    /// @param spec the pipeline wiring
    /// @return this builder
    public GatewayBuilder pipeline(PipelineId id, PipelineConfig spec) {
        pipelines.put(id, spec);
        return this;
    }

    /// Sets the service options bracketing the lifecycle.
    ///
    /// @param options the service options
    /// @return this builder
    public GatewayBuilder service(ServiceOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("service options must be non-null");
        }
        this.service = options;
        return this;
    }

    /// Compiles the registered state into a [GatewayConfig], the declarative artifact a future config
    /// loader also produces.
    ///
    /// @return the gateway config
    public GatewayConfig toConfig() {
        return new GatewayConfig(receivers, processors, exporters, connectors, pipelines, service);
    }

    /// Validates and materializes the gateway with a default component context, returning an unstarted
    /// runtime.
    ///
    /// @return the unstarted runtime
    public GatewayRuntime build() {
        return new GatewayCompiler(toConfig()).build();
    }

    /// Validates and materializes the gateway with the given base component context, returning an
    /// unstarted runtime.
    ///
    /// @param base the base component context
    /// @return the unstarted runtime
    public GatewayRuntime build(ComponentContext base) {
        return new GatewayCompiler(toConfig()).build(base);
    }
}
