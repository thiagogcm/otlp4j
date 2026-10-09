package dev.nthings.otlp4j.service.internal.graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/// The service graph: a directed acyclic graph of [Node] vertices and [Edge]s held in plain adjacency
/// maps. Nodes and edges keep their insertion order, so the fan-out numbering, the topological order
/// that drives the lifecycle, and the [#describe] dump are deterministic for a given configuration.
///
/// The graph rejects a cycle-creating edge at insertion, which is how connector cycles become
/// unrepresentable rather than checked after the fact: [#addEdge] throws a [CycleException] the
/// builder converts to a diagnostic. At most one edge joins an ordered pair of nodes.
public final class ServiceGraph {

    private record Ends(Node source, Node target) {}

    private final Map<Node, Set<Edge>> outgoing = new LinkedHashMap<>();
    private final Map<Node, Set<Edge>> incoming = new HashMap<>();
    private final Map<Edge, Ends> edges = new LinkedHashMap<>();

    /// Adds `node` as a vertex if it is not already present.
    ///
    /// @param node the node to add
    /// @return true if the node was added, false if it was already present
    public boolean addNode(Node node) {
        if (outgoing.containsKey(node)) {
            return false;
        }
        outgoing.put(node, new LinkedHashSet<>());
        incoming.put(node, new LinkedHashSet<>());
        return true;
    }

    /// Adds a typed edge from `from` to `to`. Both nodes must already be present. An edge between a
    /// pair of nodes that is already joined in that direction is ignored.
    ///
    /// @param from the source node
    /// @param to   the target node
    /// @param edge the edge to insert
    /// @throws CycleException if the edge would close a cycle
    public void addEdge(Node from, Node to, Edge edge) {
        var fromEdges = edgesOf(outgoing, from);
        var toEdges = edgesOf(incoming, to);
        if (edges.containsKey(edge)) {
            return;
        }
        if (from.equals(to) || reachable(to, this::successors).contains(from)) {
            throw new CycleException(from, to);
        }
        if (successors(from).contains(to)) {
            return;
        }
        edges.put(edge, new Ends(from, to));
        fromEdges.add(edge);
        toEdges.add(edge);
    }

    /// Removes `edge` from the graph.
    ///
    /// @param edge the edge to remove
    public void removeEdge(Edge edge) {
        var ends = edges.remove(edge);
        if (ends != null) {
            outgoing.get(ends.source()).remove(edge);
            incoming.get(ends.target()).remove(edge);
        }
    }

    /// Every vertex in the graph, in insertion order.
    ///
    /// @return the vertex set
    public Set<Node> nodes() {
        return Collections.unmodifiableSet(outgoing.keySet());
    }

    /// The outgoing edges of `node`, in insertion order.
    ///
    /// @param node the node
    /// @return its outgoing edges
    public Set<Edge> outgoing(Node node) {
        return Collections.unmodifiableSet(edgesOf(outgoing, node));
    }

    /// The target node of `edge`.
    ///
    /// @param edge the edge
    /// @return the target node
    public Node target(Edge edge) {
        return ends(edge).target();
    }

    /// The nodes reachable from `node` by following edges forward, not including `node` itself.
    ///
    /// @param node the node
    /// @return its descendants
    public Set<Node> descendants(Node node) {
        return reachable(node, this::successors);
    }

    /// The nodes that reach `node` by following edges forward, not including `node` itself.
    ///
    /// @param node the node
    /// @return its ancestors
    public Set<Node> ancestors(Node node) {
        return reachable(node, this::predecessors);
    }

    /// The forward topological order of the whole graph, as a reusable [TopoOrder] exposing both
    /// directions. Nodes without a remaining upstream are taken first in, first out, starting from the
    /// source nodes in insertion order, so the order is stable for a given configuration.
    ///
    /// @return the topological order
    public TopoOrder topoOrder() {
        var pendingUpstreams = new HashMap<Node, Integer>();
        var ready = new ArrayDeque<Node>();
        for (var node : outgoing.keySet()) {
            var upstreams = incoming.get(node).size();
            pendingUpstreams.put(node, upstreams);
            if (upstreams == 0) {
                ready.add(node);
            }
        }
        var forward = new ArrayList<Node>(outgoing.size());
        while (!ready.isEmpty()) {
            var node = ready.poll();
            forward.add(node);
            for (var next : successors(node)) {
                if (pendingUpstreams.merge(next, -1, Integer::sum) == 0) {
                    ready.add(next);
                }
            }
        }
        return new TopoOrder(forward);
    }

    /// A hand-written DOT-style dump of the graph, listing each node and each edge with its signal.
    ///
    /// @return the multi-line dump
    public String describe() {
        var sb = new StringBuilder("digraph gateway {\n");
        for (var node : topoOrder().forward()) {
            sb.append("  \"").append(node.label()).append("\";\n");
        }
        for (var entry : edges.entrySet()) {
            sb.append("  \"")
                    .append(entry.getValue().source().label())
                    .append("\" -> \"")
                    .append(entry.getValue().target().label())
                    .append("\" [signal=")
                    .append(entry.getKey().signal().signalName())
                    .append("];\n");
        }
        sb.append("}");
        return sb.toString();
    }

    private ArrayList<Node> successors(Node node) {
        var result = new ArrayList<Node>();
        for (var edge : outgoing.get(node)) {
            result.add(edges.get(edge).target());
        }
        return result;
    }

    private ArrayList<Node> predecessors(Node node) {
        var result = new ArrayList<Node>();
        for (var edge : incoming.get(node)) {
            result.add(edges.get(edge).source());
        }
        return result;
    }

    /// The nodes reached from `start` by repeatedly following `neighbours`, not including `start`.
    private Set<Node> reachable(Node start, Function<Node, ? extends Iterable<Node>> neighbours) {
        edgesOf(outgoing, start);
        var seen = new LinkedHashSet<Node>();
        var frontier = new ArrayDeque<Node>();
        frontier.add(start);
        while (!frontier.isEmpty()) {
            for (var next : neighbours.apply(frontier.poll())) {
                if (seen.add(next)) {
                    frontier.add(next);
                }
            }
        }
        return seen;
    }

    private Ends ends(Edge edge) {
        var ends = edges.get(edge);
        if (ends == null) {
            throw new IllegalArgumentException("no such edge in graph: " + edge);
        }
        return ends;
    }

    private static Set<Edge> edgesOf(Map<Node, Set<Edge>> adjacency, Node node) {
        var result = adjacency.get(node);
        if (result == null) {
            throw new IllegalArgumentException("no such node in graph: " + node);
        }
        return result;
    }
}
