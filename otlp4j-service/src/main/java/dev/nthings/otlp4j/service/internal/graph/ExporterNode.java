package dev.nthings.otlp4j.service.internal.graph;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// The graph vertex for an exporter, identified by its [ComponentId] together with the signal it
/// terminates. An exporter used for two different signals is realized as one node per signal, each a
/// per-signal facet over the shared underlying client the factory manages, which is the Java
/// expression of the shared-component guarantee. Two pipelines of the same signal that terminate on
/// the same exporter id share one exporter node.
///
/// @param id     the exporter's identity
/// @param signal the signal this exporter terminates
public record ExporterNode(ComponentId id, SignalDescriptor<?> signal) implements Node {

    /// Validates that both components are present.
    public ExporterNode {
        if (id == null || signal == null) {
            throw new IllegalArgumentException("exporter node components must be non-null");
        }
    }

    @Override
    public String label() {
        return "exporter " + id + " (" + signal + ")";
    }

    @Override
    public boolean component() {
        return true;
    }
}
