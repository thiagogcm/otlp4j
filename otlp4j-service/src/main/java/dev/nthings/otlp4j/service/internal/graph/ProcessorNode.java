package dev.nthings.otlp4j.service.internal.graph;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// The graph vertex for a processor. Unlike a receiver or exporter, a processor is not shared across
/// pipelines: each pipeline gets its own processor instance, so a processor node is identified by its
/// [ComponentId] together with the [PipelineId] it belongs to. The signal it operates on is the
/// pipeline's signal, carried here so the builder can create the processor for that signal.
///
/// @param id       the processor's identity
/// @param pipeline the pipeline this processor instance belongs to
/// @param signal   the signal this processor operates on
public record ProcessorNode(ComponentId id, PipelineId pipeline, SignalDescriptor<?> signal) implements Node {

    /// Validates that every component is present.
    public ProcessorNode {
        if (id == null || pipeline == null || signal == null) {
            throw new IllegalArgumentException("processor node components must be non-null");
        }
    }

    @Override
    public String label() {
        return "processor " + id + " [" + pipeline + "]";
    }

    @Override
    public boolean component() {
        return true;
    }
}
