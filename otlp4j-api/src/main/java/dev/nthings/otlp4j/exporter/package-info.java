/// Terminal consumers that send telemetry to an OTLP destination.
///
/// `OtlpGrpcExporter` owns one client channel and exposes a per-signal `Sink` facet
/// (`traces()`, `metrics()`, `logs()`, `profiles()`); lifecycle (`shutdown`, `forceFlush`, `close`)
/// lives on the exporter itself, not on the facets. Implement `Exporter` for a custom single-signal
/// terminal with flush and shutdown hooks.
package dev.nthings.otlp4j.exporter;
