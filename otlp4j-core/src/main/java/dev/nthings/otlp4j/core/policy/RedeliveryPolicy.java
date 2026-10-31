package dev.nthings.otlp4j.core.policy;

import java.time.Duration;

/// How a delivery queue redelivers an entry its consumer failed retryably: the backoff between
/// attempts and how long to keep trying before the entry is given up as lost. Where [RetryPolicy]
/// bounds the short retry loop inside one wire call, this policy governs the long loop around it, which
/// is what lets an acknowledged entry survive a destination outage.
///
/// The wait before attempt `n + 1` is the larger of the jittered backoff and any retry-after hint the
/// failure carried. An entry is given up as soon as that wait would run past `maxElapsed`, counted from
/// the entry's first attempt, rather than after sleeping through the end of the budget.
///
/// @param backoff    the schedule between redelivery attempts
/// @param maxElapsed the total time an entry may spend being redelivered, from its first attempt
public record RedeliveryPolicy(BackoffPolicy backoff, Duration maxElapsed) {

    private static final RedeliveryPolicy DEFAULT = new RedeliveryPolicy(
            new BackoffPolicy(Duration.ofSeconds(5), 1.5, 0.5, Duration.ofSeconds(30)), Duration.ofMinutes(5));

    /// Validates the backoff and the elapsed budget.
    public RedeliveryPolicy {
        if (backoff == null) {
            throw new IllegalArgumentException("backoff must be non-null");
        }
        if (maxElapsed == null || maxElapsed.isNegative() || maxElapsed.isZero()) {
            throw new IllegalArgumentException("maxElapsed must be > 0");
        }
    }

    /// The default redelivery policy, the upstream Collector's retry defaults: five second initial
    /// backoff, `1.5` multiplier, `0.5` jitter, thirty second cap, and five minutes in total.
    ///
    /// @return the default redelivery policy
    public static RedeliveryPolicy defaults() {
        return DEFAULT;
    }
}
