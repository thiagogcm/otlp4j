package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.LogsSink;
import dev.nthings.otlp4j.pipeline.MetricsSink;
import dev.nthings.otlp4j.pipeline.ProfilesSink;
import dev.nthings.otlp4j.pipeline.TracesSink;

/// A multi-signal OTLP exporter: one instance delivers all four signals to a single endpoint through
/// typed facets.
///
/// The per-signal facets ([#traces()], [#metrics()], [#logs()], [#profiles()]) are
/// [TracesSink]/[MetricsSink]/[LogsSink]/[ProfilesSink] views that carry the exporter's lifecycle: all
/// four share one channel, so draining any facet drains the whole exporter. Attaching a facet to a
/// pipeline therefore drains the exporter on shutdown with nothing to register. Lifecycle
/// ([#shutdown]/[#forceFlush]) also lives on the exporter itself; [#close()] it directly when a facet
/// is used outside a pipeline. Build one with `OtlpGrpcExporter` / `OtlpHttpExporter`.
///
/// For a custom single-signal destination, implement `Sink` plus [Lifecycle] directly.
public interface OtlpExporter extends Lifecycle {

    /// The trace export facet.
    TracesSink traces();

    /// The metrics export facet.
    MetricsSink metrics();

    /// The logs export facet.
    LogsSink logs();

    /// The profiles export facet.
    ProfilesSink profiles();
}
