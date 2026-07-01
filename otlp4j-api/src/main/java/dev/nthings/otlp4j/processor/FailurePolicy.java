package dev.nthings.otlp4j.processor;

/// Whether a downstream failure of derived telemetry fails the input batch.
public enum FailurePolicy {
    /// Accept the input regardless of downstream outcome (logged); the best-effort default.
    BEST_EFFORT,
    /// Propagate a downstream `Partial`/`Rejected` as a `Rejected` on the input.
    FAIL
}
