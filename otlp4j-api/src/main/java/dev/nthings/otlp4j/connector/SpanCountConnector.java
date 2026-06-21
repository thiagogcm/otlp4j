package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Consumer;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// A trace-to-metrics connector emitting `otlp4j.connector.span.count` as a monotonic delta sum.
public final class SpanCountConnector implements TraceConsumer, Connector<TraceData, MetricsData> {

    private static final Logger log = LoggerFactory.getLogger(SpanCountConnector.class);

    private final MetricConsumer downstream;

    SpanCountConnector(MetricConsumer downstream) {
        this.downstream = Objects.requireNonNull(downstream, "downstream");
    }

    @Override
    public Consumer<? super MetricsData> downstream() {
        return downstream;
    }

    @Override
    public CompletionStage<ConsumeResult<TraceData>> consume(TraceData batch) {
        var metric = CountMetrics.deltaSum(
                "otlp4j.connector.span.count", "Items observed by SpanCountConnector", batch.spans().size());
        return downstream.consume(metric).thenApply(r -> CountMetrics.acceptInput(r, log, "SpanCountConnector"));
    }
}
