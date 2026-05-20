package dev.nthings.otlp4j.spi;

import java.time.Duration;

/// Simple retry policy carried by [ClientTransportConfig].
///
/// `maxAttempts` includes the initial attempt; a value of 1 disables retries. The current
/// gRPC transport does not retry, but the policy is part of the SPI so alternate transports
/// can pick it up without changing the API.
public record RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {

    private static final RetryPolicy NONE = new RetryPolicy(1, Duration.ZERO, Duration.ZERO);

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
    }

    /// A policy that performs no retries.
    public static RetryPolicy none() {
        return NONE;
    }
}
