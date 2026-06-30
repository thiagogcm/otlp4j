/// Terminal consumers that send telemetry to an OTLP destination.
///
/// An `OtlpExporter` owns one client channel and exposes per-signal `Sink` facets
/// (`traces()`, `metrics()`, `logs()`, `profiles()`); lifecycle (`shutdown`, `forceFlush`, `close`)
/// lives on the exporter itself. Build one with `OtlpGrpcExporter` / `OtlpHttpExporter`. Implement
/// `Exporter` for a custom single-signal terminal with flush and shutdown hooks.
@NullMarked
package dev.nthings.otlp4j.exporter;

import org.jspecify.annotations.NullMarked;
