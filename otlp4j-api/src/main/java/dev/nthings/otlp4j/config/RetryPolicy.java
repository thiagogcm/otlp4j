package dev.nthings.otlp4j.config;

import java.time.Duration;
import java.util.Objects;

/// Retry policy carried by [ClientConfig].
///
/// `maxAttempts` includes the initial attempt; a value of 1 disables retries. Between attempts the
/// backoff starts at `initialBackoff` and grows by `backoffMultiplier` up to `maxBackoff`. The
/// bundled gRPC transport maps this onto gRPC's native retry (over the OTLP-recommended retryable
/// status codes) and raises gRPC's channel-level retry cap to match `maxAttempts`, so the full
/// configured budget is honoured; the per-request deadline still bounds total wall time.
public record RetryPolicy(
        int maxAttempts, Duration initialBackoff, Duration maxBackoff, double backoffMultiplier) {

    private static final RetryPolicy NONE = new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.5);
    private static final RetryPolicy DEFAULT =
            new RetryPolicy(5, Duration.ofSeconds(1), Duration.ofSeconds(5), 1.5);

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
            if (backoffMultiplier < 1.0) {
                throw new IllegalArgumentException(
                        "backoffMultiplier must be >= 1.0 when retries are enabled");
            }
        }
    }

    /// A policy that performs no retries.
    public static RetryPolicy none() {
        return NONE;
    }

    /// The default policy: 5 attempts, 1s initial backoff growing 1.5x up to 5s.
    public static RetryPolicy getDefault() {
        return DEFAULT;
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Returns a builder pre-populated with this policy's fields.
    public Builder toBuilder() {
        return new Builder()
                .setMaxAttempts(maxAttempts)
                .setInitialBackoff(initialBackoff)
                .setMaxBackoff(maxBackoff)
                .setBackoffMultiplier(backoffMultiplier);
    }

    /// Builder for [RetryPolicy]; defaults match [#getDefault()].
    public static final class Builder {

        private int maxAttempts = 5;
        private Duration initialBackoff = Duration.ofSeconds(1);
        private Duration maxBackoff = Duration.ofSeconds(5);
        private double backoffMultiplier = 1.5;

        private Builder() {}

        /// Sets the total attempts including the initial one; pass at least 2 to retry.
        public Builder setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder setInitialBackoff(Duration initialBackoff) {
            this.initialBackoff = initialBackoff;
            return this;
        }

        public Builder setMaxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
            return this;
        }

        /// Sets the factor the backoff grows by between attempts (e.g. 1.5); must be >= 1.0.
        public Builder setBackoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
            return this;
        }

        public RetryPolicy build() {
            return new RetryPolicy(maxAttempts, initialBackoff, maxBackoff, backoffMultiplier);
        }
    }
}
