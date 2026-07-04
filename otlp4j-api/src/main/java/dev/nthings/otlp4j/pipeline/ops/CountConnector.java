package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.pipeline.Sink;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

/// Counts each input batch and emits a delta-sum metric downstream. Backs [Connectors#spanCount]
/// and [Connectors#logRecordCount]; a [Connector] instance whose [#derive] builds the count
/// metric. The window stays monotonic under concurrent flushes and clock steps. The two concrete
/// signals are [SpanCountConnector] and [LogRecordCountConnector].
abstract sealed class CountConnector<I> extends Connector<I, MetricsData>
        permits SpanCountConnector, LogRecordCountConnector {

    private static final InstrumentationScope SCOPE =
            InstrumentationScope.of("otlp4j-count-connector", "0.1.0");

    private final String description;
    private final ToLongFunction<I> counter;
    private final AtomicLong previousFlushNanos = new AtomicLong(nowEpochNanos());

    CountConnector(
            Sink<? super MetricsData> downstream,
            FailurePolicy policy,
            String metricName,
            String description,
            ToLongFunction<I> counter) {
        super(metricName, downstream, policy);
        this.description = description;
        this.counter = counter;
    }

    @Override
    protected MetricsData derive(I batch) {
        var now = nowEpochNanos();
        var start = previousFlushNanos.getAndAccumulate(now, Math::max);
        return deltaSum(counter.applyAsLong(batch), start, Math.max(start, now));
    }

    /// Single-point DELTA sum over `[startEpochNanos, epochNanos)`.
    private MetricsData deltaSum(long count, long startEpochNanos, long epochNanos) {
        var point = new NumberPoint(
                Attributes.empty(), startEpochNanos, epochNanos, NumberPoint.longValue(count), 0L, List.of());
        var metric = new Metric(
                name(),
                description,
                "{item}",
                new Metric.Sum(List.of(point), Metric.AggregationTemporality.DELTA, true),
                Attributes.empty());
        return MetricsData.of(Resource.EMPTY, SCOPE, List.of(metric));
    }

    /// Nanosecond wall clock (`currentTimeMillis` is only millisecond-resolution).
    private static long nowEpochNanos() {
        var now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
