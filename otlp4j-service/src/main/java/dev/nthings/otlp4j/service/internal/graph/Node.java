package dev.nthings.otlp4j.service.internal.graph;

/// A vertex of the service graph. The five kinds mirror the component taxonomy plus the synthetic
/// fan-out vertex the builder inserts where one producer feeds several same-signal consumers. This
/// type and every implementation live in a package that no module exports, so no JGraphT type and no
/// graph vertex type ever appears in an exported signature.
///
/// A component-backed node (receiver, processor, exporter, connector) carries the identity the
/// builder instantiates it under; a [FanOutNode] is synthetic and backs no component. The [#label]
/// renders the node for the DOT-style dump the runtime's `describe` produces.
public sealed interface Node permits ReceiverNode, ProcessorNode, ExporterNode, ConnectorNode, FanOutNode {

    /// A short human-readable label for this node, used by the graph dump.
    ///
    /// @return the label
    String label();

    /// Whether this node backs a lifecycle-bearing component that is instantiated, started, and shut
    /// down. A fan-out node is not: it is a pure consumer with no lifecycle.
    ///
    /// @return true for a component-backed node, false for a synthetic fan-out node
    boolean component();
}
