package dev.nthings.otlp4j.service.config;

import java.time.Duration;

/// The service-level options that bracket the gateway lifecycle: the deadline each component's start
/// is bounded by and the deadline each component's shutdown is bounded by.
///
/// @param startDeadline    the per-component start deadline
/// @param shutdownDeadline the per-component shutdown deadline
public record ServiceOptions(Duration startDeadline, Duration shutdownDeadline) {

    private static final Duration DEFAULT_START_DEADLINE = Duration.ofSeconds(30);

    private static final Duration DEFAULT_SHUTDOWN_DEADLINE = Duration.ofSeconds(30);

    /// Validates the deadlines.
    public ServiceOptions {
        if (startDeadline == null || startDeadline.isNegative() || startDeadline.isZero()) {
            throw new IllegalArgumentException("startDeadline must be a positive duration");
        }
        if (shutdownDeadline == null || shutdownDeadline.isNegative() || shutdownDeadline.isZero()) {
            throw new IllegalArgumentException("shutdownDeadline must be a positive duration");
        }
    }

    /// The default options: a thirty-second start and shutdown deadline.
    ///
    /// @return the default service options
    public static ServiceOptions defaults() {
        return new ServiceOptions(DEFAULT_START_DEADLINE, DEFAULT_SHUTDOWN_DEADLINE);
    }
}
