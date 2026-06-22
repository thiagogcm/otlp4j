package dev.nthings.otlp4j.spi;

import java.time.Duration;
import java.util.Objects;

/// Simple retry policy carried by [ClientTransportConfig].
///
/// `maxAttempts` includes the initial attempt; a value of 1 disables retries. The bundled
/// gRPC transport maps this onto gRPC's native retry (exponential backoff over the OTLP-
/// recommended retryable status codes) and raises gRPC's channel-level retry cap to match
/// `maxAttempts`, so the full configured budget is honoured; the per-request deadline still bounds
/// total wall time.
public record RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {

    private static final RetryPolicy NONE = new RetryPolicy(1, Duration.ZERO, Duration.ZERO);

    public RetryPolicy {
        Objects.requireNonNull(initialBackoff, "initialBackoff");
        Objects.requireNonNull(maxBackoff, "maxBackoff");
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (maxAttempts > 1) {
            if (initialBackoff.isZero() || initialBackoff.isNegative()) {
                throw new IllegalArgumentException("initialBackoff must be > 0 when retries are enabled");
            }
            if (maxBackoff.compareTo(initialBackoff) < 0) {
                throw new IllegalArgumentException("maxBackoff must be >= initialBackoff");
            }
        }
    }

    /// A policy that performs no retries.
    public static RetryPolicy none() {
        return NONE;
    }

    /// An exponential-backoff policy; `maxAttempts` counts the initial attempt, so pass at least 2 to retry.
    public static RetryPolicy exponential(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
        return new RetryPolicy(maxAttempts, initialBackoff, maxBackoff);
    }
}
