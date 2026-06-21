package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.pipeline.MetricConsumer;

/// Factories for the built-in [Connector]s.
public final class Connectors {

    private Connectors() {}

    /// A trace-to-metrics connector emitting `otlp4j.connector.span.count` into `downstream`.
    public static SpanCountConnector spanCount(MetricConsumer downstream) {
        return new SpanCountConnector(downstream);
    }

    /// A logs-to-metrics connector emitting `otlp4j.connector.log.record.count` into `downstream`.
    public static LogRecordCountConnector logRecordCount(MetricConsumer downstream) {
        return new LogRecordCountConnector(downstream);
    }
}
