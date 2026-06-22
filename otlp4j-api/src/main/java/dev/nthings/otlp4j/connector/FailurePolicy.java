package dev.nthings.otlp4j.connector;

/// How a [Connector] reports a downstream delivery failure of its derived telemetry back to the
/// caller that produced the input batch.
public enum FailurePolicy {
    /// Accept the input regardless of downstream outcome; log non-Accepted results for visibility.
    /// The default: derived telemetry is best-effort and never fails the originating request.
    BEST_EFFORT,
    /// Propagate a downstream `Partial`/`Rejected` onto the input result as a `Rejected`, so the
    /// caller sees that derived telemetry was not delivered.
    FAIL
}
