package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.OptionalLong;

/// Maps [ConsumeResult.Rejected] to transport-level errors.
public final class DeliveryResults {

    private DeliveryResults() {
    }

    /// Whether a whole-batch rejection is retryable (transient).
    public static boolean isRetryable(ConsumeResult.Rejected rejected) {
        return rejected.retryable();
    }

    /// HTTP status for a whole-batch rejection: 503 when retryable, 500 when permanent.
    public static int httpStatus(ConsumeResult.Rejected rejected) {
        return isRetryable(rejected) ? 503 : 500;
    }

    /// The rejection's retry-after hint as HTTP `Retry-After` delta-seconds: empty when absent,
    /// otherwise the delay rounded up to whole seconds with a floor of 1 (a sub-second hint must
    /// not collapse to `Retry-After: 0`, which reads as "retry immediately").
    public static OptionalLong retryAfterSeconds(ConsumeResult.Rejected rejected) {
        var retryAfter = rejected.retryAfter();
        if (retryAfter == null) {
            return OptionalLong.empty();
        }
        var seconds = Math.ceilDiv(retryAfter.toNanos(), 1_000_000_000L);
        return OptionalLong.of(Math.max(1L, seconds));
    }
}
