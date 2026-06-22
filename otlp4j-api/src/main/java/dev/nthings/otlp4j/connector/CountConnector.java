package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Consumer;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Counts the items in each input batch and emits a monotonic delta-sum metric downstream.
///
/// Backs [Connectors#spanCount] and [Connectors#logRecordCount]; the metric name, description, and
/// per-batch item count are supplied at construction. Each flush carries a per-series delta window
/// running from the previous flush (the first from construction), so the series has a real per-series
/// start time. The window stays contiguous and monotonic even under concurrent `consume()` calls and
/// across a backward wall-clock step: the atomic `getAndAccumulate(now, Math::max)` orders concurrent
/// flushes and never moves the start backward, and the end is clamped to `max(start, now)` so the
/// width is never negative. A [FailurePolicy] decides whether a downstream metric failure propagates
/// onto the input result.
///
/// @param <I> the input OTLP signal whose items are counted
final class CountConnector<I> implements Connector<I, MetricsData> {

    private static final InstrumentationScope SCOPE =
            InstrumentationScope.of("otlp4j-count-connector", "0.1.0");
    private static final Logger log = LoggerFactory.getLogger(CountConnector.class);

    private final MetricConsumer downstream;
    private final FailurePolicy policy;
    private final String metricName;
    private final String description;
    private final ToLongFunction<I> counter;
    private final AtomicLong previousFlushNanos = new AtomicLong(nowEpochNanos());

    CountConnector(
            MetricConsumer downstream,
            FailurePolicy policy,
            String metricName,
            String description,
            ToLongFunction<I> counter) {
        this.downstream = Objects.requireNonNull(downstream, "downstream");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.metricName = metricName;
        this.description = description;
        this.counter = counter;
    }

    @Override
    public Consumer<? super MetricsData> downstream() {
        return downstream;
    }

    @Override
    public CompletionStage<ConsumeResult<I>> consume(I batch) {
        long now = nowEpochNanos();
        long start = previousFlushNanos.getAndAccumulate(now, Math::max);
        var metric = deltaSum(counter.applyAsLong(batch), start, Math.max(start, now));
        return downstream.consume(metric).thenApply(this::applyPolicy);
    }

    /// Builds the single-point DELTA sum for the window `[startEpochNanos, epochNanos)`.
    private MetricsData deltaSum(long count, long startEpochNanos, long epochNanos) {
        var point = new NumberPoint(
                Attributes.empty(), startEpochNanos, epochNanos, NumberPoint.longValue(count), 0L, List.of());
        var metric = new Metric(
                metricName,
                description,
                "{item}",
                new Metric.Sum(List.of(point), Metric.AggregationTemporality.DELTA, true),
                Attributes.empty());
        return MetricsData.of(Resource.EMPTY, SCOPE, List.of(metric));
    }

    /// Maps the downstream metric result back onto the input result under the policy. A cross-signal
    /// `Partial`/`Rejected` is logged either way; under [FailurePolicy#FAIL] it propagates as a
    /// `Rejected` on the input so the caller learns the derived metric was not delivered.
    private ConsumeResult<I> applyPolicy(ConsumeResult<MetricsData> downstreamResult) {
        switch (downstreamResult) {
            case ConsumeResult.Accepted<MetricsData> _ -> {
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Partial<MetricsData>(var rejected, var message) -> {
                log.warn("{} downstream partial_success: {} rejected items, msg={}", metricName, rejected, message);
                if (policy == FailurePolicy.FAIL) {
                    return ConsumeResult.rejected(
                            metricName + " derived metric partially rejected: " + rejected + " points: " + message);
                }
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Rejected<MetricsData>(var message, var cause) -> {
                log.warn("{} downstream rejected derived metric: {}", metricName, message, cause);
                if (policy == FailurePolicy.FAIL) {
                    return ConsumeResult.rejected(metricName + " derived metric rejected: " + message, cause);
                }
                return ConsumeResult.accepted();
            }
        }
    }

    /// Nanosecond wall clock — `System.currentTimeMillis()` only carries millisecond resolution.
    private static long nowEpochNanos() {
        var now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
