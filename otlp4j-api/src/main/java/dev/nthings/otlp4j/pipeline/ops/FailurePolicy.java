package dev.nthings.otlp4j.pipeline.ops;

/// Whether a downstream failure of derived telemetry fails the input batch. Part of the general
/// [Connector] contract; the bundled count connectors (see [Connectors]) expose it directly.
///
/// This is the connector's take on the same "does the sender learn the downstream outcome?" axis the
/// [Batcher] expresses with [AckMode], one non-buffering, one buffering:
///
/// | Sender learns downstream outcome? | Connector | Batcher |
/// | --- | --- | --- |
/// | Yes (propagate) | `FailurePolicy.FAIL` | `AckMode.ON_DELIVERY` |
/// | No (swallow) | `FailurePolicy.BEST_EFFORT` | `AckMode.ON_ENQUEUE` |
///
/// The types stay distinct because `AckMode` also controls delivery timing, which `FailurePolicy`
/// does not.
public enum FailurePolicy {
    /// Accept the input regardless of downstream outcome (logged); the best-effort default.
    BEST_EFFORT,
    /// Propagate a downstream `Partial`/`Rejected` as a `Rejected` on the input.
    FAIL
}
