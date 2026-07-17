package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.LongAdder;

/// The receiver's live telemetry tap: one [MulticastPublisher] per signal, mirroring every accepted
/// [TelemetryPayload] to any side-channel subscribers without back-pressuring the acknowledgement
/// path. Overflow drops feed the shared `otlp4j.receiver.tap.dropped` counter. A receiver-runtime
/// internal; it is not part of the graph runtime's public contract surface in Phase 1, so a subscriber
/// accessor is not exposed on the core surface yet, but the receiver still mirrors accepted traffic and
/// reports its drop counter through it.
public final class ReceiverTap implements AutoCloseable {

    private final LongAdder drops = new LongAdder();
    private final Map<SignalDescriptor<?>, MulticastPublisher<TelemetryPayload>> publishers = new LinkedHashMap<>();

    public ReceiverTap(GatewayMetrics telemetry) {
        var dropped = telemetry.counter(
                "otlp4j.receiver.tap.dropped", "telemetry items dropped by a tap subscriber's overflow");
        for (SignalDescriptor<?> signal : SignalDescriptor.values()) {
            publishers.put(signal, new MulticastPublisher<>(drops, dropped));
        }
    }

    /// Live payloads of `signal` under [TapOptions#defaults()].
    ///
    /// @param signal the signal to observe
    /// @return the tap publisher for that signal
    Flow.Publisher<TelemetryPayload> publisher(SignalDescriptor<?> signal) {
        return publishers.get(signal);
    }

    /// Live payloads of `signal`; `options` bind to the next subscription.
    ///
    /// @param signal  the signal to observe
    /// @param options the buffer and overflow policy bound to the next subscription
    /// @return the tap publisher for that signal
    Flow.Publisher<TelemetryPayload> publisher(SignalDescriptor<?> signal, TapOptions options) {
        var publisher = publishers.get(signal);
        return subscriber -> publisher.subscribe(subscriber, options);
    }

    /// Total number of payloads dropped across every tap publisher since the receiver started.
    ///
    /// @return the drop count
    long droppedCount() {
        return drops.sum();
    }

    /// Publishes `payload` to its signal's tap subscribers.
    ///
    /// @param payload the accepted payload to mirror
    public void publish(TelemetryPayload payload) {
        publishers.get(payload.signal()).publish(payload);
    }

    @Override
    public void close() {
        publishers.values().forEach(MulticastPublisher::close);
    }
}
