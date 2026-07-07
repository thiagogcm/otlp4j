package dev.nthings.otlp4j.service.internal.graph;

import dev.nthings.otlp4j.core.component.ComponentId;

/// The graph vertex for a receiver, identified by its [ComponentId]. A receiver is a shared
/// component: one instance per id serves every pipeline it feeds, so there is exactly one receiver
/// node per receiver id regardless of how many pipelines or signals it serves. The signals it serves
/// are read from its outgoing edges rather than stored on the node, so the same node carries a
/// multi-signal receiver.
///
/// @param id the receiver's identity
public record ReceiverNode(ComponentId id) implements Node {

    /// Validates that the id is present.
    public ReceiverNode {
        if (id == null) {
            throw new IllegalArgumentException("receiver node id must be non-null");
        }
    }

    @Override
    public String label() {
        return "receiver " + id;
    }

    @Override
    public boolean component() {
        return true;
    }
}
