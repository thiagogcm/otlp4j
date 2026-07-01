package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.pipeline.LogSink;
import dev.nthings.otlp4j.pipeline.MetricSink;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.TraceSink;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.TracesData;

/// Factories for built-in count sinks; overloads default to [FailurePolicy#BEST_EFFORT].
public final class Connectors {

    private Connectors() {}

    /// Trace count sink emitting `otlp4j.connector.span.count` into `downstream`, best-effort.
    public static TraceSink spanCount(MetricSink downstream) {
        return spanCount(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// Trace count sink emitting `otlp4j.connector.span.count` into `downstream` under `policy`.
    public static TraceSink spanCount(MetricSink downstream, FailurePolicy policy) {
        Sink<TracesData> counter = new CountConnector<>(
                downstream,
                policy,
                "otlp4j.connector.span.count",
                "Items observed by the span count connector",
                TracesData::spanCount);
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
