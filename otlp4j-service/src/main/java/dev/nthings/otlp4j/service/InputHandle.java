package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// A builder-owned application input, resolved through [GatewayRuntime#input].
/// @param <T> the domain batch type
public final class InputHandle<T> implements PipelineSource {
    private final ComponentId id;
    private final SignalDescriptor<T> signal;
    private final Object owner;

    InputHandle(ComponentId id, SignalDescriptor<T> signal, Object owner) {
        this.id = id;
        this.signal = signal;
        this.owner = owner;
    }

    @Override
    public ComponentId id() { return id; }

    public SignalDescriptor<T> signal() { return signal; }

    Object owner() { return owner; }
}
