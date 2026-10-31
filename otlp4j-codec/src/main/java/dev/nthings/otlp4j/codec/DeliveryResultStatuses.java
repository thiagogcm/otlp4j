package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import java.time.Duration;
import java.util.OptionalLong;

/// Maps a [DeliveryResult.Rejected] to transport-level errors: a retryable rejection is `503` (gRPC
/// `UNAVAILABLE`), a permanent one is `500` (gRPC `INTERNAL`), or `400` (gRPC `INVALID_ARGUMENT`) when
/// the request itself is invalid, and a retry-after hint becomes a `Retry-After` delta-seconds value.
public final class DeliveryResultStatuses {

    private DeliveryResultStatuses() {}

    /// Whether a whole-payload rejection is retryable (transient).
    ///
    /// @param rejected the rejection
    /// @return true when the disposition is retryable
    public static boolean isRetryable(DeliveryResult.Rejected rejected) {
        return rejected.disposition() == Disposition.RETRYABLE;
    }

    /// Whether a whole-payload rejection blames the request's content.
    ///
    /// @param rejected the rejection
    /// @return true when the request is invalid and retrying it cannot help
    public static boolean isInvalidRequest(DeliveryResult.Rejected rejected) {
        return !isRetryable(rejected) && rejected.reason() == DeliveryReason.PROTOCOL_TRANSLATION;
    }

    /// HTTP status for a whole-payload rejection: 503 when retryable, 400 for an invalid request, and
    /// 500 for any other permanent rejection.
    ///
    /// @param rejected the rejection
    /// @return the HTTP status code
    public static int httpStatus(DeliveryResult.Rejected rejected) {
        if (isRetryable(rejected)) {
            return 503;
        }
        return isInvalidRequest(rejected) ? 400 : 500;
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
