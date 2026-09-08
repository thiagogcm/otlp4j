package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// A component registration owned by the builder that created it.
public final class ConnectorHandle implements PipelineSource, PipelineTarget {
    private final ComponentId id;
    private final Object owner;

    ConnectorHandle(ComponentId id, Object owner) {
        this.id = id;
        this.owner = owner;
    }

    public ComponentId id() { return id; }

    Object owner() { return owner; }

    @Override
    public String toString() { return id.toString(); }
}
