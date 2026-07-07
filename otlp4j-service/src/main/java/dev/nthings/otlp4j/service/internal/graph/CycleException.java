package dev.nthings.otlp4j.service.internal.graph;

/// Thrown by [ServiceGraph#addEdge] when inserting an edge would close a cycle. It carries the two
/// nodes whose edge was refused so the builder can render a `CYCLE_DETECTED` diagnostic that names
/// them. JGraphT's own cycle signal is caught inside the graph and never escapes as a JGraphT type;
/// this domain exception is what the builder sees.
public final class CycleException extends RuntimeException {

    private final transient Node from;

    private final transient Node to;

    /// Builds the exception for the refused edge from `from` to `to`.
    ///
    /// @param from the edge's source node
    /// @param to   the edge's target node
    public CycleException(Node from, Node to) {
        super("edge " + from.label() + " -> " + to.label() + " would create a cycle");
        this.from = from;
        this.to = to;
    }

    /// The refused edge's source node.
    ///
    /// @return the source node
    public Node from() {
        return from;
    }

    /// The refused edge's target node.
    ///
    /// @return the target node
    public Node to() {
        return to;
    }
}
