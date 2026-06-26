package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.MetricSink;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Counts each input batch and emits a delta-sum metric downstream, backing [Connectors#spanCount]
/// and [Connectors#logRecordCount]. The window runs from the previous flush and stays monotonic
/// under concurrent flushes and a backward clock step (`getAndAccumulate(now, Math::max)`, end
/// clamped to `max(start, now)`); [FailurePolicy] governs whether a downstream failure fails the input.
final class CountConnector<I> implements Sink<I> {

    private static final InstrumentationScope SCOPE =
            InstrumentationScope.of("otlp4j-count-connector", "0.1.0");
    private static final Logger log = LoggerFactory.getLogger(CountConnector.class);

    private final MetricSink downstream;
    private final FailurePolicy policy;
    private final String metricName;
    private final String description;
    private final ToLongFunction<I> counter;
    private final AtomicLong previousFlushNanos = new AtomicLong(nowEpochNanos());

    CountConnector(
            MetricSink downstream,
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
    public CompletionStage<ConsumeResult<I>> consume(I batch) {
        var now = nowEpochNanos();
        var start = previousFlushNanos.getAndAccumulate(now, Math::max);
        var metric = deltaSum(counter.applyAsLong(batch), start, Math.max(start, now));
        // Normalize a synchronous throw or failed stage to a Rejected so applyPolicy governs the input.
        CompletionStage<ConsumeResult<MetricsData>> downstreamStage;
        try {
            downstreamStage = downstream.consume(metric);
        } catch (RuntimeException e) {
            downstreamStage = CompletableFuture.completedFuture(rejectedDownstream("threw", e));
        }
        return downstreamStage
                .exceptionally(t -> rejectedDownstream("failed", t))
                .thenApply(this::applyPolicy);
    }

    /// Permanent rejection for the derived metric, unwrapping [CompletionException] to the real cause.
    private ConsumeResult<MetricsData> rejectedDownstream(String verb, Throwable failure) {
        var cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause()
                : failure;
        return ConsumeResult.permanentRejected(metricName + " downstream " + verb + ": " + cause, cause);
    }

    /// Single-point DELTA sum over `[startEpochNanos, epochNanos)`.
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

    /// Maps the downstream metric result onto the input result; logs failures, and under
    /// [FailurePolicy#FAIL] propagates them as a `Rejected` on the input.
    private ConsumeResult<I> applyPolicy(ConsumeResult<MetricsData> downstreamResult) {
        switch (downstreamResult) {
            case ConsumeResult.Accepted<MetricsData> _ -> {
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Partial<MetricsData>(var rejected, var message) -> {
                log.warn("{} downstream partial_success: {} rejected items, msg={}", metricName, rejected, message);
                if (policy == FailurePolicy.FAIL) {
                    return ConsumeResult.retryableRejected(
                            metricName + " derived metric partially rejected: " + rejected + " points: " + message);
                }
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Rejected<MetricsData>(var message, var cause) -> {
                log.warn("{} downstream rejected derived metric: {}", metricName, message, cause);
                if (policy == FailurePolicy.FAIL) {
                    // Forward the downstream cause verbatim; it decides retry intent.
                    return ConsumeResult.rejected(metricName + " derived metric rejected: " + message, cause);
                }
                return ConsumeResult.accepted();
            }
        }
    }

    /// Nanosecond wall clock (`currentTimeMillis` is only millisecond-resolution).
    private static long nowEpochNanos() {
        var now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
