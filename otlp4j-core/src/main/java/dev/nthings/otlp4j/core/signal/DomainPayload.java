package dev.nthings.otlp4j.core.signal;

import java.util.Objects;

/// Package-private immutable implementation behind [Payload#of].
final class DomainPayload<T> implements Payload<T> {

    private final SignalDescriptor<T> signal;
    private final T data;
    private final RoutingMetadata routingMetadata;

    DomainPayload(SignalDescriptor<T> signal, T data) {
        this.signal = Objects.requireNonNull(signal, "signal");
        this.data = Objects.requireNonNull(data, "data");
        this.routingMetadata = signal.routingMetadata(data);
    }

    @Override
    public SignalDescriptor<T> signal() {
        return signal;
    }

    @Override
    public T data() {
        return data;
    }

    @Override
    public long itemCount() {
        return signal.itemCount(data);
    }

    @Override
    public RoutingMetadata routingMetadata() {
        return routingMetadata;
    }

    @Override
    public Payload<T> withData(T replacement) {
        Objects.requireNonNull(replacement, "replacement");
        return replacement == data ? this : new DomainPayload<>(signal, replacement);
    }

    @Override
    public String toString() {
        return "Payload(" + signal.signalName() + ", items=" + itemCount() + ")";
    }
}
