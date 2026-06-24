package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.TraceData;

/// Factories for the built-in count sinks; no-policy overloads default to [FailurePolicy#BEST_EFFORT].
public final class Connectors {

    private Connectors() {}

    /// Trace count sink emitting `otlp4j.connector.span.count` into `downstream`, best-effort.
    public static TraceSink spanCount(MetricSink downstream) {
        return spanCount(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// Trace count sink emitting `otlp4j.connector.span.count` into `downstream` under `policy`.
    public static TraceSink spanCount(MetricSink downstream, FailurePolicy policy) {
        Sink<TraceData> counter = new CountConnector<>(
                downstream,
                policy,
                "otlp4j.connector.span.count",
                "Items observed by the span count connector",
                TraceData::spanCount);
        return counter::consume;
    }

    /// Log count sink emitting `otlp4j.connector.log.record.count` into `downstream`, best-effort.
    public static LogSink logRecordCount(MetricSink downstream) {
        return logRecordCount(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// Log count sink emitting `otlp4j.connector.log.record.count` into `downstream` under `policy`.
    public static LogSink logRecordCount(MetricSink downstream, FailurePolicy policy) {
        Sink<LogsData> counter = new CountConnector<>(
                downstream,
                policy,
                "otlp4j.connector.log.record.count",
                "Items observed by the log record count connector",
                LogsData::logRecordCount);
        return counter::consume;
    }
}
