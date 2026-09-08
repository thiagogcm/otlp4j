package dev.nthings.otlp4j.core.policy;

import java.time.Duration;

/// The backoff schedule between retry attempts: an exponential backoff with jitter, capped. The
/// wait after attempt `n` starts at `initial`, grows by `multiplier` each attempt, is perturbed by
/// up to `jitter` fraction, and never exceeds `max`. A transport maps this neutral schedule onto its
/// own retry runtime.
///
/// @param initial    the base wait before the first retry
/// @param multiplier the per-attempt growth factor, at least `1.0`
/// @param jitter     the jitter fraction in `[0, 1]`
/// @param max        the cap on any single wait
public record BackoffPolicy(Duration initial, double multiplier, double jitter, Duration max) {

    private static final BackoffPolicy DEFAULT =
            new BackoffPolicy(Duration.ofSeconds(1), 1.5, 0.5, Duration.ofSeconds(5));

    /// Validates the numeric ranges and durations.
    public BackoffPolicy {
        if (initial == null || initial.isNegative() || initial.isZero()) {
            throw new IllegalArgumentException("initial backoff must be > 0");
        }
        if (max == null || max.compareTo(initial) < 0) {
            throw new IllegalArgumentException("max backoff must be >= initial");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0, got " + multiplier);
        }
        if (jitter < 0.0 || jitter > 1.0) {
            throw new IllegalArgumentException("jitter must be in [0, 1], got " + jitter);
        }
    }

    /// The default backoff: one second initial, `1.5` multiplier, `0.5` jitter, five second cap.
    ///
    /// @return the default backoff policy
    public static BackoffPolicy defaults() {
        return DEFAULT;
    }
}
