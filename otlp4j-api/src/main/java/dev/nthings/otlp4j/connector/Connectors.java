package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.pipeline.MetricConsumer;

/// Factories for the built-in [Connector]s.
///
/// Each connector takes an optional [FailurePolicy]; the no-policy overloads default to
/// [FailurePolicy#BEST_EFFORT], where derived telemetry never fails the originating request.
public final class Connectors {

    private Connectors() {}

    /// A trace-to-metrics connector emitting `otlp4j.connector.span.count` into `downstream`,
    /// best-effort (a downstream metric failure does not fail the input trace batch).
    public static SpanCountConnector spanCount(MetricConsumer downstream) {
        return new SpanCountConnector(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// A trace-to-metrics connector emitting `otlp4j.connector.span.count` into `downstream` under
    /// the given [FailurePolicy].
    public static SpanCountConnector spanCount(MetricConsumer downstream, FailurePolicy policy) {
        return new SpanCountConnector(downstream, policy);
    }

    /// A logs-to-metrics connector emitting `otlp4j.connector.log.record.count` into `downstream`,
    /// best-effort (a downstream metric failure does not fail the input log batch).
    public static LogRecordCountConnector logRecordCount(MetricConsumer downstream) {
        return new LogRecordCountConnector(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// A logs-to-metrics connector emitting `otlp4j.connector.log.record.count` into `downstream`
    /// under the given [FailurePolicy].
    public static LogRecordCountConnector logRecordCount(MetricConsumer downstream, FailurePolicy policy) {
        return new LogRecordCountConnector(downstream, policy);
    }
}
