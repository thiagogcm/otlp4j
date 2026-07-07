package dev.nthings.otlp4j.core.component;

import java.time.Duration;

/// The context for a `forceFlush`: the deadline within which a buffering component must push its
/// buffered telemetry downstream. It carries the same deadline the legacy `Lifecycle.forceFlush`
/// took as a bare `Duration`, wrapped so the flush surface can widen without a signature break.
///
/// @param deadline the flush deadline
public record FlushContext(Duration deadline) {

    /// Validates that the deadline is present and non-negative.
    public FlushContext {
        if (deadline == null || deadline.isNegative()) {
            throw new IllegalArgumentException("flush deadline must be non-null and non-negative");
        }
    }
}
