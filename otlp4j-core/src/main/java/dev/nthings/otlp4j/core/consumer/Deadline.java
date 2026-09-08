package dev.nthings.otlp4j.core.consumer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/// An absolute deadline for a consume call: the instant by which the work must complete. A deadline
/// is read against a `Clock` so time-dependent behaviour is testable with a fixed or offset clock.
/// This is distinct from the bare `Duration` the flush and shutdown contexts carry, because a
/// consume call needs an absolute instant it can measure remaining time against as it flows through
/// several consumers.
///
/// @param at the instant the deadline elapses
public record Deadline(Instant at) {

    /// Validates that the instant is present.
    public Deadline {
        if (at == null) {
            throw new IllegalArgumentException("deadline instant must be non-null");
        }
    }

    /// A deadline `budget` from now, read against `clock`.
    ///
    /// @param budget how far in the future the deadline is
    /// @param clock  the clock to read now from
    /// @return the deadline
    public static Deadline after(Duration budget, Clock clock) {
        if (budget == null || budget.isNegative()) {
            throw new IllegalArgumentException("budget must be non-null and non-negative");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must be non-null");
        }
        return new Deadline(clock.instant().plus(budget));
    }

    /// A deadline that does not expire during normal operation.
    ///
    /// @return the maximum representable instant
    public static Deadline never() {
        return new Deadline(Instant.MAX);
    }

    /// The time remaining until this deadline as of `clock`, which may be zero or negative once the
    /// deadline has elapsed.
    ///
    /// @param clock the clock to read now from
    /// @return the remaining duration
    public Duration remaining(Clock clock) {
        if (clock == null) {
            throw new IllegalArgumentException("clock must be non-null");
        }
        return Duration.between(clock.instant(), at);
    }

    /// Whether this deadline has elapsed as of `clock`.
    ///
    /// @param clock the clock to read now from
    /// @return true once now is at or past the deadline
    public boolean isExpired(Clock clock) {
        if (clock == null) {
            throw new IllegalArgumentException("clock must be non-null");
        }
        return !clock.instant().isBefore(at);
    }
}
