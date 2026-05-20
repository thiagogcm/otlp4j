package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Consumer;
import dev.nthings.otlp4j.pipeline.LogConsumer;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// A logs-to-metrics connector emitting `otlp4j.connector.log.record.count` as a monotonic
/// delta sum.
public final class LogRecordCountConnector implements LogConsumer, Connector<LogsData, MetricsData> {

    private static final Logger log = LoggerFactory.getLogger(LogRecordCountConnector.class);

    private final MetricConsumer downstream;

    public LogRecordCountConnector(MetricConsumer downstream) {
        this.downstream = Objects.requireNonNull(downstream, "downstream");
    }

    @Override
    public Consumer<? super MetricsData> downstream() {
        return downstream;
    }

    @Override
    public CompletionStage<ConsumeResult<LogsData>> consume(LogsData batch) {
        var metric = CountMetrics.deltaSum(
                "otlp4j.connector.log.record.count",
                "Items observed by LogRecordCountConnector",
                batch.logRecords().size());
        return downstream.consume(metric).thenApply(r -> CountMetrics.acceptInput(r, log, "LogRecordCountConnector"));
    }
}
