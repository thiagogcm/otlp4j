package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.pipeline.LogsSink;
import dev.nthings.otlp4j.pipeline.MetricsSink;

/// The log-record-count connector as a [LogsSink]: counts log records per batch and emits
/// `otlp4j.connector.log.record.count` to its downstream metric sink, whose lifecycle it cascades.
final class LogRecordCountConnector extends CountConnector<LogsData> implements LogsSink {

    LogRecordCountConnector(MetricsSink downstream, FailurePolicy policy) {
        super(
                downstream,
                policy,
                "otlp4j.connector.log.record.count",
                "Items observed by the log record count connector",
                LogsData::logRecordCount);
    }
}
