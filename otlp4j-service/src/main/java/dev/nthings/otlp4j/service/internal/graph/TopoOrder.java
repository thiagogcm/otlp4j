package dev.nthings.otlp4j.service.internal.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/// A materialized topological ordering of the service graph, exposed in both directions. The forward
/// order runs sources (receivers) first and sinks (exporters) last, following the data-flow
/// direction of the edges. The reverse order is its mirror. One ordering walked in both directions is
/// all the runtime needs: instantiation and start walk it in reverse (so a downstream exists before
/// the upstream that feeds it), and shutdown walks it forward (so receivers stop first and in-flight
/// work drains into still-open exporters).
public final class TopoOrder {

    private final List<Node> forward;

    private final List<Node> reverse;

    /// Builds the ordering from a forward topological list.
    ///
    /// @param forward the nodes in forward topological order
    public TopoOrder(List<Node> forward) {
        this.forward = List.copyOf(forward);
        var r = new ArrayList<>(this.forward);
        Collections.reverse(r);
        this.reverse = Collections.unmodifiableList(r);
    }

    /// The forward topological order: receivers first, exporters last.
    ///
    /// @return the forward order
    public List<Node> forward() {
        return forward;
    }

    /// The reverse topological order: exporters first, receivers last.
    ///
    /// @return the reverse order
    public List<Node> reverse() {
        return reverse;
    }
}
