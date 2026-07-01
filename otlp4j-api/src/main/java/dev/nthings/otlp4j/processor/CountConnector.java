package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.internal.SharedLifecycle;
import java.time.Duration;
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

/// Counts each input batch and emits a delta-sum metric downstream. Backs [Connectors#spanCount]
/// and [Connectors#logRecordCount]. The window stays monotonic under concurrent flushes and clock
/// steps; [FailurePolicy] governs downstream failure propagation.
///
/// The connector owns its single downstream: it cascades `shutdown`/`forceFlush` to it (when the
/// downstream is itself a [Lifecycle]) and propagates the pipeline's retain, so attaching the
/// connector drains its downstream automatically with no `Stage.owns(...)` declaration. The two
/// concrete signals are [SpanCountConnector] and [LogRecordCountConnector].
sealed class CountConnector<I> implements Sink<I>, SharedLifecycle
        permits SpanCountConnector, LogRecordCountConnector {

    private static final InstrumentationScope SCOPE =
            InstrumentationScope.of("otlp4j-count-connector", "0.1.0");
    private static final Logger log = LoggerFactory.getLogger(CountConnector.class);

    private final Sink<? super MetricsData> downstream;
    private final FailurePolicy policy;
    private final String metricName;
    private final String description;
    private final ToLongFunction<I> counter;
    private final AtomicLong previousFlushNanos = new AtomicLong(nowEpochNanos());

    CountConnector(
            Sink<? super MetricsData> downstream,
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
    public CompletionStage<ConsumeResult> consume(I batch) {
        var now = nowEpochNanos();
        var start = previousFlushNanos.getAndAccumulate(now, Math::max);
        var metric = deltaSum(counter.applyAsLong(batch), start, Math.max(start, now));
        // Normalize a throw or failed stage so applyPolicy governs the input.
        CompletionStage<ConsumeResult> downstreamStage;
        try {
            downstreamStage = Objects.requireNonNull(downstream.consume(metric), "downstream returned a null stage");
        } catch (Throwable e) {
            downstreamStage = CompletableFuture.completedFuture(rejectedDownstream("threw", e));
        }
        return downstreamStage
                .exceptionally(t -> rejectedDownstream("failed", t))
                .thenApply(this::applyPolicy);
    }

    @Override
    public void retain() {
        // Propagate ownership so a shared downstream stays reference-counted when reached only through us.
        if (downstream instanceof SharedLifecycle shared) {
            shared.retain();
        }
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        return downstream instanceof Lifecycle d ? d.shutdown(timeout) : CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> forceFlush(Duration timeout) {
        return downstream instanceof Lifecycle d ? d.forceFlush(timeout) : CompletableFuture.completedFuture(null);
    }

    /// Permanent rejection from a downstream failure, unwrapping [CompletionException] to the cause.
    /// An [Error] is rethrown (not swallowed past `applyPolicy`); an [InterruptedException] restores
    /// the interrupt flag.
    private ConsumeResult rejectedDownstream(String verb, Throwable failure) {
        var cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause()
                : failure;
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return ConsumeResult.permanent(metricName + " downstream " + verb + ": " + cause, cause);
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
    private ConsumeResult applyPolicy(ConsumeResult downstreamResult) {
        switch (downstreamResult) {
            case ConsumeResult.Accepted _ -> {
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Partial(var rejected, var message) -> {
                log.warn("{} downstream partial_success: {} rejected items, msg={}", metricName, rejected, message);
                if (policy == FailurePolicy.FAIL) {
                    return ConsumeResult.retryable(
                            metricName + " derived metric partially rejected: " + rejected + " points: " + message);
                }
                return ConsumeResult.accepted();
            }
            case ConsumeResult.Rejected(var retryable, var message, var cause) -> {
                log.warn("{} downstream rejected derived metric: {}", metricName, message, cause);
                if (policy == FailurePolicy.FAIL) {
                    // Forward the downstream retry intent and cause onto the input.
                    var forwarded = metricName + " derived metric rejected: " + message;
                    return retryable
                            ? ConsumeResult.retryable(forwarded, cause)
                            : ConsumeResult.permanent(forwarded, cause);
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
