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

/// The small fluent assembler [Gateway#builder] returns. It collects component instances by id and
/// pipelines by id, then compiles to a [GatewayConfig], which is always the intermediate form. There
/// is no second construction path: the assembler is sugar over building one config, and the
/// [GatewayBuilder] takes it from there. The convenience [#build] and [#build(ComponentContext)]
/// methods run the builder for a caller who does not need the intermediate config.
public final class GatewayAssembler {

    private final Map<ComponentId, ReceiverEntry<?>> receivers = new LinkedHashMap<>();

    private final Map<ComponentId, ProcessorEntry<?>> processors = new LinkedHashMap<>();

    private final Map<ComponentId, ExporterEntry<?>> exporters = new LinkedHashMap<>();

    private final Map<ComponentId, ConnectorEntry<?>> connectors = new LinkedHashMap<>();

    private final Map<PipelineId, PipelineConfig> pipelines = new LinkedHashMap<>();

    private ServiceOptions service = ServiceOptions.defaults();

    GatewayAssembler() {}

    /// Registers a receiver instance under `id`.
    ///
    /// @param id      the component id
    /// @param factory the receiver factory
    /// @param config  the receiver config
    /// @param <C>     the config type
    /// @return this assembler
    public <C extends ComponentConfig> GatewayAssembler receiver(
            ComponentId id, ReceiverFactory<C> factory, C config) {
        receivers.put(id, new ReceiverEntry<>(factory, config));
        return this;
    }

    /// Registers a processor instance under `id`.
    ///
    /// @param id      the component id
    /// @param factory the processor factory
    /// @param config  the processor config
    /// @param <C>     the config type
    /// @return this assembler
    public <C extends ComponentConfig> GatewayAssembler processor(
            ComponentId id, ProcessorFactory<C> factory, C config) {
        processors.put(id, new ProcessorEntry<>(factory, config));
        return this;
    }

    /// Registers an exporter instance under `id`.
    ///
    /// @param id      the component id
    /// @param factory the exporter factory
    /// @param config  the exporter config
    /// @param <C>     the config type
    /// @return this assembler
    public <C extends ComponentConfig> GatewayAssembler exporter(
            ComponentId id, ExporterFactory<C> factory, C config) {
        exporters.put(id, new ExporterEntry<>(factory, config));
        return this;
    }

    /// Registers a connector instance under `id`.
    ///
    /// @param id      the component id
    /// @param factory the connector factory
    /// @param config  the connector config
    /// @param <C>     the config type
    /// @return this assembler
    public <C extends ComponentConfig> GatewayAssembler connector(
            ComponentId id, ConnectorFactory<C> factory, C config) {
        connectors.put(id, new ConnectorEntry<>(factory, config));
        return this;
    }

    /// Adds a pipeline under `id`.
    ///
    /// @param id   the pipeline id
    /// @param spec the pipeline wiring
    /// @return this assembler
    public GatewayAssembler pipeline(PipelineId id, PipelineConfig spec) {
        pipelines.put(id, spec);
        return this;
    }

    /// Sets the service options bracketing the lifecycle.
    ///
    /// @param options the service options
    /// @return this assembler
    public GatewayAssembler service(ServiceOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("service options must be non-null");
        }
        this.service = options;
        return this;
    }

    /// Compiles the assembled state into a [GatewayConfig].
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
        return new GatewayBuilder(toConfig()).build();
    }

    /// Validates and materializes the gateway with the given base component context, returning an
    /// unstarted runtime.
    ///
    /// @param base the base component context
    /// @return the unstarted runtime
    public GatewayRuntime build(ComponentContext base) {
        return new GatewayBuilder(toConfig()).build(base);
    }
}
