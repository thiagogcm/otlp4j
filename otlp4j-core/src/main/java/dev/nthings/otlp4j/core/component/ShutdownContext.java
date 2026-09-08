package dev.nthings.otlp4j.core.component;

import java.time.Duration;

/// The context for a `shutdown`: the deadline within which a component must drain in-flight work and
/// release its resources.
///
/// @param deadline the shutdown deadline
public record ShutdownContext(Duration deadline) {

    /// Validates that the deadline is present and non-negative.
    public ShutdownContext {
        if (deadline == null || deadline.isNegative()) {
            throw new IllegalArgumentException("shutdown deadline must be non-null and non-negative");
        }
    }
}
