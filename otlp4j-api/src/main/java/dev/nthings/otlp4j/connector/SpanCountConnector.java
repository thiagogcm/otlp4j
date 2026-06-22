package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Consumer;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// A trace-to-metrics connector emitting `otlp4j.connector.span.count` as a monotonic delta sum.
///
/// Each flush carries a per-series delta window running from the previous flush (the first from the
/// connector's construction); the window is contiguous and monotonic even under concurrent
/// `consume()` calls and across a backward wall-clock step (see [CountMetrics#advanceWindow]). A
/// configurable [FailurePolicy] decides whether a downstream metric failure is propagated onto the
/// input result.
public final class SpanCountConnector implements TraceConsumer, Connector<TraceData, MetricsData> {

    private static final Logger log = LoggerFactory.getLogger(SpanCountConnector.class);

    private final MetricConsumer downstream;
    private final FailurePolicy policy;
    private final AtomicLong previousFlushNanos = new AtomicLong(CountMetrics.nowEpochNanos());

    SpanCountConnector(MetricConsumer downstream, FailurePolicy policy) {
        this.downstream = Objects.requireNonNull(downstream, "downstream");
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    @Override
    public Consumer<? super MetricsData> downstream() {
        return downstream;
    }

    @Override
    public CompletionStage<ConsumeResult<TraceData>> consume(TraceData batch) {
        long[] window = CountMetrics.advanceWindow(previousFlushNanos);
        var metric = CountMetrics.deltaSum(
                "otlp4j.connector.span.count",
                "Items observed by SpanCountConnector",
                batch.spans().size(),
                window[0],
                window[1]);
        return downstream.consume(metric).thenApply(r -> CountMetrics.applyPolicy(r, policy, log, "SpanCountConnector"));
    }
}
