package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.MetricConsumer;

/// Factories for the built-in count [Connector]s.
///
/// Each connector takes an optional [FailurePolicy]; the no-policy overloads default to
/// [FailurePolicy#BEST_EFFORT], where derived telemetry never fails the originating request.
public final class Connectors {

    private Connectors() {}

    /// A trace-to-metrics connector emitting `otlp4j.connector.span.count` into `downstream`,
    /// best-effort (a downstream metric failure does not fail the input trace batch).
    public static Connector<TraceData, MetricsData> spanCount(MetricConsumer downstream) {
        return spanCount(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// A trace-to-metrics connector emitting `otlp4j.connector.span.count` into `downstream` under
    /// the given [FailurePolicy].
    public static Connector<TraceData, MetricsData> spanCount(MetricConsumer downstream, FailurePolicy policy) {
        return new CountConnector<>(
                downstream,
                policy,
                "otlp4j.connector.span.count",
                "Items observed by the span count connector",
                Connectors::countSpans);
    }

    /// A logs-to-metrics connector emitting `otlp4j.connector.log.record.count` into `downstream`,
    /// best-effort (a downstream metric failure does not fail the input log batch).
    public static Connector<LogsData, MetricsData> logRecordCount(MetricConsumer downstream) {
        return logRecordCount(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// A logs-to-metrics connector emitting `otlp4j.connector.log.record.count` into `downstream`
    /// under the given [FailurePolicy].
    public static Connector<LogsData, MetricsData> logRecordCount(MetricConsumer downstream, FailurePolicy policy) {
        return new CountConnector<>(
                downstream,
                policy,
                "otlp4j.connector.log.record.count",
                "Items observed by the log record count connector",
                Connectors::countLogRecords);
    }

    // Sum the per-scope counts instead of `spans()` / `logRecords()`, whose flattened accessors
    // would materialize a full copy of every item just to read its size on each consumed batch.

    private static long countSpans(TraceData traces) {
        return traces.resourceSpans().stream()
                .flatMap(rs -> rs.scopeSpans().stream())
                .mapToInt(ss -> ss.spans().size())
                .sum();
    }

    private static long countLogRecords(LogsData logs) {
        return logs.resourceLogs().stream()
                .flatMap(rl -> rl.scopeLogs().stream())
                .mapToInt(sl -> sl.logRecords().size())
                .sum();
    }
}
