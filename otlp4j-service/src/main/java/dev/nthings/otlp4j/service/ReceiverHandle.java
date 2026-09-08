package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// A component registration owned by the builder that created it.
public final class ReceiverHandle implements PipelineSource {
    private final ComponentId id;
    private final Object owner;

    ReceiverHandle(ComponentId id, Object owner) {
        this.id = id;
        this.owner = owner;
    }

    public ComponentId id() { return id; }

    Object owner() { return owner; }

    @Override
    public String toString() { return id.toString(); }
}
