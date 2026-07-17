package dev.nthings.otlp4j.service.config;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import java.util.Map;

/// The whole declarative description of a gateway: the component instances available by id, grouped
/// by kind, the pipelines that wire them, and the service options that bracket the lifecycle. Every
/// construction path compiles to this record; the fluent builder returned by `Gateway.builder()` is
/// sugar over building one, and `Gateway.compile(GatewayConfig)` materializes a directly-built one.
///
/// The four component maps are keyed by [ComponentId], so a component is instantiated exactly once
/// per id even when several pipelines reference it. The pipelines map is keyed by [PipelineId], the
/// signal-plus-optional-name pipeline identity. The materialization step resolves each referenced id
/// against these maps for the existence and signal-support queries the validator asks.
///
/// @param receivers  the receivers available by id
/// @param processors the processors available by id
/// @param exporters  the exporters available by id
/// @param connectors the connectors available by id
/// @param pipelines  the pipelines wiring the components, by pipeline id
/// @param service    the service options bracketing the lifecycle
public record GatewayConfig(
        Map<ComponentId, ReceiverEntry<?>> receivers,
        Map<ComponentId, ProcessorEntry<?>> processors,
        Map<ComponentId, ExporterEntry<?>> exporters,
        Map<ComponentId, ConnectorEntry<?>> connectors,
        Map<PipelineId, PipelineConfig> pipelines,
        ServiceOptions service) {

    /// Defensively copies every map and validates that no component is null.
    public GatewayConfig {
        if (receivers == null
                || processors == null
                || exporters == null
                || connectors == null
                || pipelines == null) {
            throw new IllegalArgumentException("component maps and pipelines must be non-null");
        }
        if (service == null) {
            throw new IllegalArgumentException("service options must be non-null");
        }
        receivers = Map.copyOf(receivers);
        processors = Map.copyOf(processors);
        exporters = Map.copyOf(exporters);
        connectors = Map.copyOf(connectors);
        pipelines = Map.copyOf(pipelines);
    }
}
