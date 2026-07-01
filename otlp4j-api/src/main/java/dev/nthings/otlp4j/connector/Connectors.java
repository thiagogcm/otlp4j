package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.pipeline.LogsSink;
import dev.nthings.otlp4j.pipeline.MetricsSink;
import dev.nthings.otlp4j.pipeline.TracesSink;

/// Factories for built-in count sinks; overloads default to [FailurePolicy#BEST_EFFORT].
///
/// Each returned sink is a `Lifecycle` that cascades shutdown to its `downstream` metric sink, so a
/// pipeline drains that downstream automatically when the connector is attached as a terminal or
/// fan-out peer.
public final class Connectors {

    private Connectors() {}

    /// Trace count sink emitting `otlp4j.connector.span.count` into `downstream`, best-effort.
    public static TracesSink spanCount(MetricsSink downstream) {
        return spanCount(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// Trace count sink emitting `otlp4j.connector.span.count` into `downstream` under `policy`.
    public static TracesSink spanCount(MetricsSink downstream, FailurePolicy policy) {
        return new SpanCountConnector(downstream, policy);
    }

    /// Log count sink emitting `otlp4j.connector.log.record.count` into `downstream`, best-effort.
    public static LogsSink logRecordCount(MetricsSink downstream) {
        return logRecordCount(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// Log count sink emitting `otlp4j.connector.log.record.count` into `downstream` under `policy`.
    public static LogsSink logRecordCount(MetricsSink downstream, FailurePolicy policy) {
        return new LogRecordCountConnector(downstream, policy);
    }
}
