/// Connectors derive telemetry of one signal from another.
///
/// A `Connector` consumes one signal and emits a different one to its downstream `Consumer` —
/// unlike a `Transform`, it may change the signal type. Build the bundled count connectors through
/// the `Connectors` factory (`spanCount`, `logRecordCount`).
package dev.nthings.otlp4j.connector;
