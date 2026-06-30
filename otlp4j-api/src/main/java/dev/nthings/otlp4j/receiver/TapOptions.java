package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.core.OverflowPolicy;
import java.util.Objects;

/// Subscription options for a [TelemetryTap].
public record TapOptions(OverflowPolicy strategy, int bufferSize) {

    private static final TapOptions DEFAULT = new TapOptions(OverflowPolicy.DROP_OLDEST, 256);

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
