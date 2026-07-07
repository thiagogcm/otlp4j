package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.Component;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;

/// A built component paired with the kind and id it was instantiated under, the unit the runtime
/// starts and stops in order. Package-private: it is an internal runtime record, not part of the
/// exported surface.
///
/// The kind is carried alongside the id because a receiver and an exporter (or any two components of
/// different kinds) may legitimately share the same id in a Collector-style configuration, so the id
/// alone does not identify a live component. The runtime keys its component lookup by the composite
/// [#key()].
///
/// @param kind      the component's kind
/// @param id        the component's identity
/// @param component the built component instance
record ComponentRef(ComponentKind kind, ComponentId id, Component component) {

    /// The composite lookup key for this reference: its kind together with its id.
    ///
    /// @return the (kind, id) key
    ComponentKey key() {
        return new ComponentKey(kind, id);
    }
}
