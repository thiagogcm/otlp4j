package dev.nthings.otlp4j.receiver;

import java.util.Objects;

/// Subscription options for a [TelemetryTap].
///
/// Every overflow strategy is non-blocking: the tap sits outside the acknowledgement path, so a
/// slow subscriber can never delay the OTLP response (there is deliberately no `BLOCK` option).
public record TapOptions(Overflow strategy, int bufferSize) {

    /// Policy for an item that arrives while a tap subscription's bounded buffer is full. Each
    /// option resolves immediately - none blocks the receiver's publish thread.
    public enum Overflow {
        /// Evict the oldest buffered item to make room for the new one.
        DROP_OLDEST,
        /// Drop the arriving item; the tap increments its drop counter.
        DROP_NEWEST,
        /// Terminate this subscription with `onError`; other subscriptions are unaffected.
        FAIL
    }

    private static final TapOptions DEFAULT = new TapOptions(Overflow.DROP_OLDEST, 256);

    public TapOptions {
        Objects.requireNonNull(strategy, "strategy");
        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must be >= 1, got " + bufferSize);
        }
    }

    /// Default options: `DROP_OLDEST` with a 256-element buffer.
    public static TapOptions defaults() {
        return DEFAULT;
    }
}
