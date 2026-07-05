package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.SignalBatch;
import dev.nthings.otlp4j.receiver.TapOptions;
import dev.nthings.otlp4j.receiver.TelemetryTap;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.LongAdder;

/// [TelemetryTap] implementation backed by per-signal [MulticastPublisher]s.
final class ReceiverTap implements TelemetryTap, AutoCloseable {

    private final LongAdder drops = new LongAdder();
    private final Map<Signal<?>, MulticastPublisher<?>> publishers = new LinkedHashMap<>();

    ReceiverTap(GatewayMetrics telemetry) {
        var dropped = telemetry.counter(
                "otlp4j.receiver.tap.dropped", "telemetry items dropped by a tap subscriber's overflow");
        for (Signal<?> signal : Signal.values()) {
            publishers.put(signal, new MulticastPublisher<>(drops, dropped));
        }
    }

    /// The publisher for `signal`. The map is keyed by [Signal] identity and each entry was created
    /// for that signal, so the value's element type is exactly the signal's batch type.
    private <T> MulticastPublisher<T> publisherFor(Signal<T> signal) {
        @SuppressWarnings("unchecked")
        var publisher = (MulticastPublisher<T>) publishers.get(signal);
        return publisher;
    }

    @Override
    public <T> Flow.Publisher<T> publisher(Signal<T> signal) {
        return publisherFor(signal);
    }

    @Override
    public <T> Flow.Publisher<T> publisher(Signal<T> signal, TapOptions options) {
        var publisher = publisherFor(signal);
        return subscriber -> publisher.subscribe(subscriber, options);
    }

    @Override
    public long droppedCount() {
        return drops.sum();
    }

    public <T> void publish(SignalBatch<T> batch) {
        publisherFor(batch.signal()).publish(batch.batch());
    }

    @Override
    public void close() {
        publishers.values().forEach(MulticastPublisher::close);
    }
}
