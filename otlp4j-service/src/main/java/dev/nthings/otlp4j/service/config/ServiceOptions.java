package dev.nthings.otlp4j.service.config;

import dev.nthings.otlp4j.core.component.ComponentId;
import java.time.Duration;
import java.util.List;

/// The service-level options that bracket the gateway lifecycle: the deadline each component's start
/// is bounded by, the deadline each component's shutdown is bounded by, and the ordered ids of the
/// extensions the service starts before, and stops after, the data-path components.
///
/// @param startDeadline    the per-component start deadline
/// @param shutdownDeadline the per-component shutdown deadline
/// @param extensions       the extension ids to start first and stop last, in order
public record ServiceOptions(Duration startDeadline, Duration shutdownDeadline, List<ComponentId> extensions) {

    private static final Duration DEFAULT_START_DEADLINE = Duration.ofSeconds(30);

    private static final Duration DEFAULT_SHUTDOWN_DEADLINE = Duration.ofSeconds(30);

    /// Validates the deadlines and defensively copies the extension list.
    public ServiceOptions {
        if (startDeadline == null || startDeadline.isNegative() || startDeadline.isZero()) {
            throw new IllegalArgumentException("startDeadline must be a positive duration");
        }
        if (shutdownDeadline == null || shutdownDeadline.isNegative() || shutdownDeadline.isZero()) {
            throw new IllegalArgumentException("shutdownDeadline must be a positive duration");
        }
        if (extensions == null) {
            throw new IllegalArgumentException("extensions must be non-null; use an empty list for none");
        }
        extensions = List.copyOf(extensions);
    }

    /// The default options: a thirty-second start and shutdown deadline and no extensions.
    ///
    /// @return the default service options
    public static ServiceOptions defaults() {
        return new ServiceOptions(DEFAULT_START_DEADLINE, DEFAULT_SHUTDOWN_DEADLINE, List.of());
    }
}
