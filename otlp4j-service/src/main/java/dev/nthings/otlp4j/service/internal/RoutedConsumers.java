package dev.nthings.otlp4j.service.internal;

import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.Map;
import java.util.Set;

/// The concrete [SignalConsumers] bundle the builder assembles for a receiver: a map from each signal
/// the receiver serves to the head consumer of the pipeline (or the fan-out over several pipelines)
/// that signal feeds. The receiver factory receives this so one receiver instance serves every routed
/// signal from one port.
///
/// The per-signal consumers are stored behind a wildcard and recovered with a checked cast in
/// [#consumer]: the map is populated so that the consumer stored under a `SignalDescriptor<T>` is a
/// `SignalConsumer<T>`, an invariant the builder upholds and this class documents, so the single
/// unchecked cast is sound.
public final class RoutedConsumers implements SignalConsumers {

    private final Map<SignalDescriptor<?>, SignalConsumer<?>> bySignal;

    /// Builds the bundle from a signal-to-consumer map. The map is defensively copied.
    ///
    /// @param bySignal the per-signal consumers, each typed to match its key
    public RoutedConsumers(Map<SignalDescriptor<?>, SignalConsumer<?>> bySignal) {
        this.bySignal = Map.copyOf(bySignal);
    }

    @Override
    public Set<SignalDescriptor<?>> signals() {
        return bySignal.keySet();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends TelemetryPayload> SignalConsumer<T> consumer(SignalDescriptor<T> signal) {
        var consumer = bySignal.get(signal);
        if (consumer == null) {
            throw new IllegalArgumentException("no consumer routed for signal " + signal);
        }
        // Sound by construction: the builder stores a SignalConsumer<T> under a SignalDescriptor<T>.
        return (SignalConsumer<T>) consumer;
    }
}
