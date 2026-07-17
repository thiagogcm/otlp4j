package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// The typed handle a [GatewayBuilder#connector] registration returns: a thin value carrying the
/// registered connector's [ComponentId]. A connector is the only kind that bridges two pipelines, so
/// its handle is both a [PipelineSource] and a [PipelineTarget]: it is a [PipelineBuilder#to] target of
/// the pipeline that feeds it and a [PipelineBuilder#from] source of the pipeline it emits into.
///
/// @param id the id the connector was registered under
public record ConnectorHandle(ComponentId id) implements PipelineSource, PipelineTarget {

    /// Validates that the id is present.
    public ConnectorHandle {
        if (id == null) {
            throw new IllegalArgumentException("connector handle id must be non-null");
        }
    }
}
