package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.pipeline.LogSink;
import dev.nthings.otlp4j.pipeline.MetricSink;

/// The log-record-count connector as a [LogSink]: counts log records per batch and emits
/// `otlp4j.connector.log.record.count` to its downstream metric sink, whose lifecycle it cascades.
final class LogRecordCountConnector extends CountConnector<LogsData> implements LogSink {

    LogRecordCountConnector(MetricSink downstream, FailurePolicy policy) {
        super(
                downstream,
                policy,
                "otlp4j.connector.log.record.count",
                "Items observed by the log record count connector",
                LogsData::logRecordCount);
    }
}
