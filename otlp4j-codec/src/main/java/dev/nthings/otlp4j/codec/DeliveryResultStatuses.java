package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import java.time.Duration;
import java.util.OptionalLong;

/// Maps a [DeliveryResult.Rejected] to transport-level errors: a retryable rejection is `503` (gRPC
/// `UNAVAILABLE`), a permanent one is `500` (gRPC `INTERNAL`), and a retry-after hint becomes a
/// `Retry-After` delta-seconds value.
public final class DeliveryResultStatuses {

    private DeliveryResultStatuses() {}

    /// Whether a whole-payload rejection is retryable (transient).
    ///
    /// @param rejected the rejection
    /// @return true when the disposition is retryable
    public static boolean isRetryable(DeliveryResult.Rejected rejected) {
        return rejected.disposition() == Disposition.RETRYABLE;
    }

    /// HTTP status for a whole-payload rejection: 503 when retryable, 500 when permanent.
    ///
    /// @param rejected the rejection
    /// @return the HTTP status code
    public static int httpStatus(DeliveryResult.Rejected rejected) {
        return isRetryable(rejected) ? 503 : 500;
    }

    /// The rejection's retry-after hint as HTTP `Retry-After` delta-seconds: empty when absent,
    /// otherwise [#retryAfterSeconds(Duration)].
    ///
    /// @param rejected the rejection
    /// @return the delta-seconds, or empty when there is no hint
    public static OptionalLong retryAfterSeconds(DeliveryResult.Rejected rejected) {
        var retryAfter = rejected.retryAfter();
        return retryAfter == null ? OptionalLong.empty() : OptionalLong.of(retryAfterSeconds(retryAfter));
    }

    /// A retry-after hint as HTTP `Retry-After` delta-seconds: rounded up to whole seconds with a
    /// floor of 1 (a sub-second hint must not collapse to `Retry-After: 0`, which reads as
    /// "retry immediately").
    ///
    /// @param retryAfter the hint duration
    /// @return the delta-seconds, at least 1
    public static long retryAfterSeconds(Duration retryAfter) {
        return Math.max(1L, Math.ceilDiv(retryAfter.toNanos(), 1_000_000_000L));
    }
}
