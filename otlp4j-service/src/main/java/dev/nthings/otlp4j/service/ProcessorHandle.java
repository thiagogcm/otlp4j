package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// The typed handle a [GatewayBuilder#processor] registration returns: a thin value carrying the
/// registered processor's [ComponentId], with the component kind captured in its type so it can be
/// used only in a pipeline's ordered processor chain ([PipelineBuilder#via]). It is neither a
/// [PipelineSource] nor a [PipelineTarget], so wiring it as a pipeline's head or terminal is a compile
/// error.
///
/// @param id the id the processor was registered under
public record ProcessorHandle(ComponentId id) {

    /// Validates that the id is present.
    public ProcessorHandle {
        if (id == null) {
            throw new IllegalArgumentException("processor handle id must be non-null");
        }
    }
}
