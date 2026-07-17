package dev.nthings.otlp4j.service.internal.graph;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

/// The service graph, wrapping a JGraphT [DirectedAcyclicGraph] of [Node] vertices and [Edge]s. The
/// JGraphT type is confined entirely to this class in a package no module exports, so no JGraphT type
/// ever appears in an exported signature; if the module-path exposure of JGraphT's transitive
/// dependencies is ever judged unacceptable, this class can be reimplemented over a hand-rolled
/// adjacency structure behind the same surface with no change visible to any caller.
///
/// The DAG rejects a cycle-creating edge at insertion, which is how connector cycles become
/// unrepresentable rather than checked after the fact: [#addEdge] catches that rejection and rethrows
/// it as a [CycleException] the builder converts to a diagnostic.
public final class ServiceGraph {

    private final DirectedAcyclicGraph<Node, Edge> dag = new DirectedAcyclicGraph<>(null, null, false);

    /// Adds `node` as a vertex if it is not already present.
    ///
    /// @param node the node to add
    /// @return true if the node was added, false if it was already present
    public boolean addNode(Node node) {
        return dag.addVertex(node);
    }

    /// Adds a typed edge from `from` to `to`. Both nodes must already be present.
    ///
    /// @param from the source node
    /// @param to   the target node
    /// @param edge the edge to insert
    /// @throws CycleException if the edge would close a cycle
    public void addEdge(Node from, Node to, Edge edge) {
        try {
            dag.addEdge(from, to, edge);
        } catch (IllegalArgumentException cycle) {
            // DirectedAcyclicGraph.addEdge throws IllegalArgumentException when the edge would induce
            // a cycle; the vertices are always present here, so this is the only cause.
            throw new CycleException(from, to);
        }
    }

    /// Removes `edge` from the graph.
    ///
    /// @param edge the edge to remove
    public void removeEdge(Edge edge) {
        dag.removeEdge(edge);
    }

    /// Every vertex in the graph.
    ///
    /// @return the vertex set
    public Set<Node> nodes() {
        return dag.vertexSet();
    }

    /// The outgoing edges of `node`.
    ///
    /// @param node the node
    /// @return its outgoing edges
    public Set<Edge> outgoing(Node node) {
        return dag.outgoingEdgesOf(node);
    }

    /// The target node of `edge`.
    ///
    /// @param edge the edge
    /// @return the target node
    public Node target(Edge edge) {
        return dag.getEdgeTarget(edge);
    }

    /// The nodes reachable from `node` by following edges forward, not including `node` itself.
    ///
    /// @param node the node
    /// @return its descendants
    public Set<Node> descendants(Node node) {
        return dag.getDescendants(node);
    }

    /// The nodes that reach `node` by following edges forward, not including `node` itself.
    ///
    /// @param node the node
    /// @return its ancestors
    public Set<Node> ancestors(Node node) {
        return dag.getAncestors(node);
    }

    /// The forward topological order of the whole graph, as a reusable [TopoOrder] exposing both
    /// directions.
    ///
    /// @return the topological order
    public TopoOrder topoOrder() {
        var forward = new ArrayList<Node>(dag.vertexSet().size());
        new TopologicalOrderIterator<>(dag).forEachRemaining(forward::add);
        return new TopoOrder(forward);
    }

    /// A hand-written DOT-style dump of the graph, listing each node and each edge with its signal.
    /// This replaces the fluent pipeline's description without pulling in `jgrapht-io`.
    ///
    /// @return the multi-line dump
    public String describe() {
        var ordered = new LinkedHashSet<Node>();
        topoOrder().forward().forEach(ordered::add);
        ordered.addAll(dag.vertexSet());
        var sb = new StringBuilder("digraph gateway {\n");
        for (var node : ordered) {
            sb.append("  \"").append(node.label()).append("\";\n");
        }
        var edges = new ArrayList<>(dag.edgeSet());
        for (var edge : edges) {
            sb.append("  \"")
                    .append(dag.getEdgeSource(edge).label())
                    .append("\" -> \"")
                    .append(dag.getEdgeTarget(edge).label())
                    .append("\" [signal=")
                    .append(edge.signal().signalName())
                    .append("];\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
