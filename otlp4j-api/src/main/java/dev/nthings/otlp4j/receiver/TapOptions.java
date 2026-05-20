package dev.nthings.otlp4j.receiver;

import java.util.Objects;

/// Subscription options for a [TelemetryTap].
public record TapOptions(BackpressureStrategy strategy, int bufferSize) {

    private static final TapOptions DEFAULT = new TapOptions(BackpressureStrategy.DROP_OLDEST, 256);

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
