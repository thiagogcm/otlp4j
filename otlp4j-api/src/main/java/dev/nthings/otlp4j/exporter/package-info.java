/// Terminal consumers that send telemetry to an OTLP destination.
///
/// `OtlpGrpcExporter` owns one client channel and exposes per-signal `Sink` facets
/// (`traces()`, `metrics()`, `logs()`, `profiles()`) that delegate lifecycle (`shutdown`,
/// `forceFlush`, `close`) to the owning exporter. Implement `Exporter` for a custom single-signal
/// terminal with flush and shutdown hooks.
package dev.nthings.otlp4j.exporter;
