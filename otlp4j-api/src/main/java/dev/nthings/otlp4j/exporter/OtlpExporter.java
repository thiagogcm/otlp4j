package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.ForceFlushable;
import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.TraceSink;

/// A multi-signal OTLP exporter: one instance delivers all four signals to a single endpoint through
/// typed facets.
///
/// The per-signal facets ([#traces()], [#metrics()], [#logs()], [#profiles()]) are plain
/// [TraceSink]/[MetricSink]/[LogSink]/[ProfileSink] views with no lifecycle of their own; lifecycle
/// ([#shutdown]/[#forceFlush]) lives on the exporter. Register it with `Stage.owns(exporter)` or
/// `Stage.to(exporter.traces(), exporter)`, or [#close()] it directly when a facet is used outside a
/// pipeline. Build one with `OtlpGrpcExporter` / `OtlpHttpExporter`.
///
/// [Exporter] is the single-signal terminal for a custom destination.
public interface OtlpExporter extends Drainable, ForceFlushable {

    /// The trace export facet.
    TraceSink traces();

    /// The metrics export facet.
    MetricSink metrics();

    /// The logs export facet.
    LogSink logs();

    /// The profiles export facet.
    ProfileSink profiles();
}
