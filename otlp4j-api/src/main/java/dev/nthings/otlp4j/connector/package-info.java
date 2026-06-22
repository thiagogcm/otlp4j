/// Connectors derive telemetry of one signal from another.
///
/// A `Connector` consumes one signal and emits a different one to its downstream `Consumer` —
/// unlike a `Transform`, it may change the signal type. Build the bundled count connectors through
/// the `Connectors` factory (`spanCount`, `logRecordCount`).
///
/// The count connectors emit a DELTA sum whose per-series window start is the previous flush, and
/// expose a configurable [FailurePolicy] (default `BEST_EFFORT`)
/// governing whether a downstream metric failure is propagated onto the input result.
package dev.nthings.otlp4j.connector;
