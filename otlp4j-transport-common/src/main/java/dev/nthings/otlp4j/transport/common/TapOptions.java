package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import java.util.Objects;

/// Subscription options for a [ReceiverTap] publisher.
///
/// Reuses the neutral [OverflowPolicy] as the one overflow vocabulary. Every tap strategy is
/// non-blocking: the tap sits outside the acknowledgement path, so a slow subscriber can never delay
/// the OTLP response. [OverflowPolicy#BLOCK] is therefore rejected at construction (it is a
/// batch-processor-only ingress-backpressure policy); use `DROP_OLDEST`, `DROP_NEWEST`, or `FAIL`.
///
/// @param strategy   the overflow policy for a full subscription buffer (not [OverflowPolicy#BLOCK])
/// @param bufferSize the per-subscription buffer size, at least 1
public record TapOptions(OverflowPolicy strategy, int bufferSize) {

    private static final TapOptions DEFAULT = new TapOptions(OverflowPolicy.DROP_OLDEST, 256);

    public TapOptions {
        Objects.requireNonNull(strategy, "strategy");
        if (strategy == OverflowPolicy.BLOCK) {
            throw new IllegalArgumentException(
                    "the tap sits outside the ack path; BLOCK is unavailable, use DROP_OLDEST, DROP_NEWEST, or FAIL");
        }
        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must be >= 1, got " + bufferSize);
        }
    }

    /// Default options: `DROP_OLDEST` with a 256-element buffer.
    public static TapOptions defaults() {
        return DEFAULT;
    }
}
