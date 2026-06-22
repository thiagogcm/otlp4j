package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;

/// Shared envelope-building and downstream-result handling for count connectors.
final class CountMetrics {

    private static final InstrumentationScope SCOPE =
            InstrumentationScope.of("otlp4j-count-connector", "0.1.0");

    private CountMetrics() {}

    /// Builds the single-point DELTA sum for `[startEpochNanos, epochNanos)` — the window that
    /// elapsed since the previous flush, so the series carries a real per-series start time.
    static MetricsData deltaSum(
            String name, String description, long count, long startEpochNanos, long epochNanos) {
        var point = new NumberPoint(
                Attributes.empty(), startEpochNanos, epochNanos, NumberPoint.longValue(count), 0L, List.of());
        var metric = new Metric(
                name,
                description,
                "{item}",
                new Metric.Sum(List.of(point), Metric.AggregationTemporality.DELTA, true),
                Attributes.empty());
        return MetricsData.of(Resource.EMPTY, SCOPE, List.of(metric));
    }

    /// Nanosecond wall clock — `System.currentTimeMillis()` only carries millisecond resolution.
    static long nowEpochNanos() {
        var now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }

    /// Advances the per-series delta window, returning `{start, end}`. `start` is the previous flush
    /// time and `end = max(now, start)`, so the window width is never negative: a backward wall-clock
    /// step (e.g. an NTP correction) or a concurrent flush that already advanced the clock yields a
    /// zero-width window rather than an inverted (`start > end`) one, and the series start never moves
    /// backward. The `getAndAccumulate` is atomic, so concurrent flushes stay ordered — no two observe
    /// the same `start`. The first window after construction runs from the connector's creation time.
    static long[] advanceWindow(AtomicLong previousFlushNanos) {
        long now = nowEpochNanos();
        long start = previousFlushNanos.getAndAccumulate(now, Math::max);
        return new long[] {start, Math.max(start, now)};
    }

    /// Maps the downstream metric result back onto the input result under `policy`.
    ///
    /// Cross-signal `Partial`/`Rejected` from the derived metric is logged for visibility either
    /// way. Under [FailurePolicy#BEST_EFFORT] the input is still accepted; under
    /// [FailurePolicy#FAIL] the failure is propagated as a `Rejected` on the input result so the
    /// caller learns the derived telemetry was not delivered.
    static <I> ConsumeResult<I> applyPolicy(
            ConsumeResult<MetricsData> downstream, FailurePolicy policy, Logger log, String connectorName) {
        switch (downstream) {
            case ConsumeResult.Accepted<MetricsData> _ -> {
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Partial<MetricsData>(var rejected, var message) -> {
                log.warn("{} downstream partial_success: {} rejected items, msg={}",
                        connectorName, rejected, message);
                if (policy == FailurePolicy.FAIL) {
                    return ConsumeResult.rejected(connectorName
                            + " derived metric partially rejected: " + rejected + " points: " + message);
                }
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Rejected<MetricsData>(var message, var cause) -> {
                log.warn("{} downstream rejected derived metric: {}", connectorName, message, cause);
                if (policy == FailurePolicy.FAIL) {
                    return ConsumeResult.rejected(connectorName + " derived metric rejected: " + message, cause);
                }
                return ConsumeResult.accepted();
            }
        }
    }
}
