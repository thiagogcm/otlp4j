package dev.nthings.otlp4j.core.policy;

import java.time.Duration;

/// The timeout policy for a delivery: the per-attempt timeout and the total time budget across all
/// attempts and backoffs. A single attempt is abandoned after `perAttempt`; the whole retrying
/// delivery is abandoned after `total`.
///
/// @param perAttempt the timeout for one attempt
/// @param total      the total budget across all attempts, at least `perAttempt`
public record TimeoutPolicy(Duration perAttempt, Duration total) {

    /// Validates that both durations are positive and the total is at least one attempt.
    public TimeoutPolicy {
        if (perAttempt == null || perAttempt.isNegative() || perAttempt.isZero()) {
            throw new IllegalArgumentException("perAttempt timeout must be > 0");
        }
        if (total == null || total.compareTo(perAttempt) < 0) {
            throw new IllegalArgumentException("total timeout must be >= perAttempt");
        }
    }
}
