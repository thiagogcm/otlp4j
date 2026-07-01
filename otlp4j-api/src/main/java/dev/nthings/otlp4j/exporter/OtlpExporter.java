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
/// The per-signal facets ([#traces()], [#metrics()], [#logs()], [#profiles()]) are
/// [TraceSink]/[MetricSink]/[LogSink]/[ProfileSink] views that carry the exporter's lifecycle: all
/// four share one channel, so draining any facet drains the whole exporter. Attaching a facet to a
/// pipeline therefore drains the exporter on shutdown with nothing to register. Lifecycle
/// ([#shutdown]/[#forceFlush]) also lives on the exporter itself; [#close()] it directly when a facet
/// is used outside a pipeline. Build one with `OtlpGrpcExporter` / `OtlpHttpExporter`.
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
