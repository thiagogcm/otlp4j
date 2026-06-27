package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.model.ConsumeResult;

/// Shared delivery semantics for mapping [ConsumeResult.Rejected] to
/// transport errors.
public final class DeliveryResults {

    private DeliveryResults() {
    }

    /// Whether a whole-batch rejection should be treated as retryable (transient)
    /// by the transport.
    public static boolean isRetryable(ConsumeResult.Rejected<?> rejected) {
        return rejected.cause() == null;
    }

    /// HTTP status for a whole-batch rejection: `503` when retryable, `500`
    /// when permanent.
    public static int httpStatus(ConsumeResult.Rejected<?> rejected) {
        return isRetryable(rejected) ? 503 : 500;
    }
}
