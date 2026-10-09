package dev.nthings.otlp4j.service.internal.graph;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// A typed edge of the service graph: it carries the signal that flows along it. For a connector edge
/// the signal is the pair's output side, the signal the connector emits into its receiving pipeline.
///
/// This is a class with identity semantics rather than a record on purpose. The graph keys its edges
/// by the edge object's `equals`/`hashCode`. Two edges carrying the same signal descriptor between
/// different node pairs must remain distinct graph edges, so each edge is a fresh object distinguished
/// by identity, which value-based record equality would collapse.
public final class Edge {

    private final SignalDescriptor<?> signal;

    /// Builds an edge carrying `signal`.
    ///
    /// @param signal the signal that flows along this edge
    public Edge(SignalDescriptor<?> signal) {
        if (signal == null) {
            throw new IllegalArgumentException("edge signal must be non-null");
        }
        this.signal = signal;
    }

    /// The signal that flows along this edge.
    ///
    /// @return the signal descriptor
    public SignalDescriptor<?> signal() {
        return signal;
    }

    @Override
    public String toString() {
        return signal.signalName();
    }
}
