package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.LogSink;
import dev.nthings.otlp4j.pipeline.MetricSink;
import dev.nthings.otlp4j.pipeline.ProfileSink;
import dev.nthings.otlp4j.pipeline.TraceSink;

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
/// For a custom single-signal destination, implement `Sink` plus [Lifecycle] directly.
public interface OtlpExporter extends Lifecycle {

    /// The trace export facet.
    TraceSink traces();

    /// The metrics export facet.
    MetricSink metrics();

    /// The logs export facet.
    LogSink logs();

    /// The profiles export facet.
    ProfileSink profiles();
}
