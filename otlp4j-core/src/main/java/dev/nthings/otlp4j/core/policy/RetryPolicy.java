package dev.nthings.otlp4j.core.policy;

/// The retry policy for a delivery: how many attempts to make, the backoff schedule between them,
/// and whether to respect a server's retry-after hint. A transport maps this neutral policy onto
/// its own request/retry runtime.
///
/// @param maxAttempts            the total number of attempts, at least one
/// @param backoff                the backoff schedule between attempts
/// @param respectServerRetryAfter whether to honour a server-sent retry-after hint
public record RetryPolicy(int maxAttempts, BackoffPolicy backoff, boolean respectServerRetryAfter) {

    private static final RetryPolicy DEFAULT = new RetryPolicy(5, BackoffPolicy.defaults(), true);

    /// Validates the attempt count and backoff.
    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
        }
        if (backoff == null) {
            throw new IllegalArgumentException("backoff must be non-null");
        }
    }

    /// The default retry policy: five attempts, the default backoff, honouring the server hint.
    ///
    /// @return the default retry policy
    public static RetryPolicy defaults() {
        return DEFAULT;
    }
}
