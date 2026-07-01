package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.model.ConsumeResult;

/// Maps [ConsumeResult.Rejected] to transport-level errors.
public final class DeliveryResults {

    private DeliveryResults() {
    }

    /// Whether a whole-batch rejection is retryable (transient).
    public static boolean isRetryable(ConsumeResult.Rejected<?> rejected) {
        return rejected.retryable();
    }

    /// HTTP status for a whole-batch rejection: 503 when retryable, 500 when permanent.
    public static int httpStatus(ConsumeResult.Rejected<?> rejected) {
        return isRetryable(rejected) ? 503 : 500;
    }
}
