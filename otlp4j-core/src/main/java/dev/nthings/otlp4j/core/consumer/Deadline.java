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
        return new Deadline(clock.instant().plus(budget));
    }

    /// The time remaining until this deadline as of `clock`, which may be zero or negative once the
    /// deadline has elapsed.
    ///
    /// @param clock the clock to read now from
    /// @return the remaining duration
    public Duration remaining(Clock clock) {
        return Duration.between(clock.instant(), at);
    }

    /// Whether this deadline has elapsed as of `clock`.
    ///
    /// @param clock the clock to read now from
    /// @return true once now is at or past the deadline
    public boolean isExpired(Clock clock) {
        return !clock.instant().isBefore(at);
    }
}
