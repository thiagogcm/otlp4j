package dev.nthings.otlp4j.service.internal.graph;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import dev.nthings.otlp4j.core.component.ComponentId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/// The graph rules the gateway compiler and runtime rely on: a cycle is refused at the edge that would
/// close it, one edge joins an ordered pair of nodes, and the topological order is the same for the
/// same sequence of insertions.
class ServiceGraphTest {

    private final ServiceGraph graph = new ServiceGraph();
    private final Node a = node("a");
    private final Node b = node("b");
    private final Node c = node("c");
    private final Node d = node("d");

    @Test
    void refusesTheEdgeThatWouldCloseACycleThroughOtherNodes() {
        link(a, b);
        link(b, c);

        var refused = catchThrowableOfType(CycleException.class, () -> link(c, a));

        assertThat(refused.from()).isEqualTo(c);
        assertThat(refused.to()).isEqualTo(a);
        assertThat(graph.outgoing(c)).isEmpty();
        assertThat(graph.topoOrder().forward()).containsExactly(a, b, c);
    }

    @Test
    void refusesAnEdgeFromANodeToItself() {
        graph.addNode(a);

        assertThat(catchThrowableOfType(CycleException.class, () -> graph.addEdge(a, a, new Edge(TRACES))))
                .isNotNull();
        assertThat(graph.outgoing(a)).isEmpty();
    }

    @Test
    void acceptsAnEdgeOnceTheEdgeThatMadeItACycleIsRemoved() {
        var forward = link(a, b);
        graph.removeEdge(forward);

        link(b, a);

        assertThat(graph.topoOrder().forward()).containsExactly(b, a);
    }

    @Test
    void keepsOneEdgeBetweenAnOrderedPairOfNodes() {
        var first = link(a, b);
        link(a, b);

        assertThat(graph.outgoing(a)).containsExactly(first);
        assertThat(graph.describe()).containsOnlyOnce("\"receiver a\" -> \"receiver b\"");
    }

    @Test
    void ordersSourcesByInsertionThenEachNodeAfterAllItsUpstreams() {
        // d is inserted first and has no upstream; c waits for both a and b.
        graph.addNode(d);
        link(a, c);
        link(b, c);
        link(a, b);

        var order = graph.topoOrder();

        assertThat(order.forward()).containsExactly(d, a, b, c);
        assertThat(order.reverse()).containsExactly(c, b, a, d);
    }

    @Test
    void reachabilityFollowsEdgesInEachDirectionAndExcludesTheNodeItself() {
        link(a, b);
        link(b, c);
        graph.addNode(d);

        assertThat(graph.descendants(a)).containsExactlyInAnyOrder(b, c);
        assertThat(graph.ancestors(c)).containsExactlyInAnyOrder(a, b);
        assertThat(graph.descendants(d)).isEmpty();
        assertThat(graph.ancestors(a)).isEmpty();
    }

    @Test
    void listsOutgoingTargetsInInsertionOrder() {
        link(a, c);
        link(a, b);

        var targets = new ArrayList<Node>();
        graph.outgoing(a).forEach(edge -> targets.add(graph.target(edge)));

        assertThat(targets).isEqualTo(List.of(c, b));
    }

    private Edge link(Node from, Node to) {
        graph.addNode(from);
        graph.addNode(to);
        var edge = new Edge(TRACES);
        graph.addEdge(from, to, edge);
        return edge;
    }

    private static Node node(String id) {
        return new ReceiverNode(ComponentId.parse(id));
    }
}
