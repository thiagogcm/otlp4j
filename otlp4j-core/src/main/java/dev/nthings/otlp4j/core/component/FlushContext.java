package dev.nthings.otlp4j.core.component;

import java.time.Duration;

/// The context for [Component#flush(FlushContext)]: the deadline within which a buffering component
/// must push its buffered telemetry downstream.
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
