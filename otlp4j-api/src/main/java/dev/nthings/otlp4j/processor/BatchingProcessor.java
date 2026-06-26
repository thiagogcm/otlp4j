package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.Flushable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.Nullable;

/// An asynchronous, queue-backed, timer-triggered batching processor.
///
/// Flushes downstream when either the size threshold or `maxBatchAge` is reached. As a [Drainable]
/// and [Flushable], a directly-attached batcher drains within a pipeline's shared shutdown budget
/// and participates in `forceFlush`.
///
/// @param <T> the OTLP signal carried by this batcher
public final class BatchingProcessor<T> implements Sink<T>, Drainable, Flushable {

    private static final Logger log = LoggerFactory.getLogger(BatchingProcessor.class);

    private final Sink<? super T> downstream;
    private final int maxBatchSize;
    private final ArrayBlockingQueue<T> queue;
    private final DropPolicy dropPolicy;
    private final LongAdder drops;
    private final ScheduledExecutorService scheduler;
    private final boolean ownsScheduler;
    private final ScheduledFuture<?> timer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Merger<T> merger;

    /// OTLP item count (spans / data points / log records / profiles) for a batch, so a dropped
    /// batch reports a true partial-success count, not a Java-batch count of one.
    private final ToLongFunction<T> itemCounter;

    /// Drains run serially here; the timer (on `scheduler`) only decides when to flush.
    private final ExecutorService drainExecutor;

    /// Each drain chains after the previous (guarded by `drainMutex`); the tail is error-isolated
    /// so one failure doesn't wedge the next.
    private final Object drainMutex = new Object();
    private CompletableFuture<Void> drainTail = CompletableFuture.completedFuture(null);

    private BatchingProcessor(Builder<T> b) {
        this.downstream = Objects.requireNonNull(b.downstream, "downstream");
        this.maxBatchSize = b.maxBatchSize;
        this.queue = new ArrayBlockingQueue<>(b.queueCapacity);
        this.dropPolicy = b.dropPolicy;
        this.drops = b.drops == null ? new LongAdder() : b.drops;
        this.merger = b.merger;
        this.itemCounter = b.itemCounter;
        this.ownsScheduler = b.scheduler == null;
        this.scheduler = b.scheduler == null
                ? Executors.newSingleThreadScheduledExecutor(
                        Thread.ofPlatform().daemon().name("otlp4j-batcher").factory())
                : b.scheduler;
        this.drainExecutor = Executors.newSingleThreadExecutor(
                Thread.ofPlatform().daemon().name("otlp4j-batcher-drain").factory());
        this.timer = scheduler.scheduleAtFixedRate(this::flushIfDue, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public CompletionStage<ConsumeResult<T>> consume(T batch) {
        if (closed.get()) {
            return CompletableFuture.completedFuture(ConsumeResult.retryableRejected("batcher closed"));
        }
        var offered = queue.offer(batch);
        if (!offered) {
            switch (dropPolicy) {
                case DROP_OLDEST -> {
                    var dropped = queue.poll();
                    if (dropped != null) {
                        drops.increment();
                    }
                    queue.offer(batch);
                }
                case DROP_NEWEST -> {
                    drops.increment();
                    // Report the rejected OTLP item count, not a Java-batch count of one.
                    var rejectedItems = itemCounter.applyAsLong(batch);
                    return CompletableFuture.completedFuture(rejectedItems == 0
                            ? ConsumeResult.accepted()
                            : ConsumeResult.partial(rejectedItems, "batcher queue full"));
                }
                case BLOCK -> {
                    try {
                        queue.put(batch);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return CompletableFuture.completedFuture(
                                ConsumeResult.permanentRejected("interrupted waiting for batcher capacity", e));
                    }
                }
                case ERROR -> {
                    drops.increment();
                    return CompletableFuture.completedFuture(
                            ConsumeResult.retryableRejected("batcher queue full"));
                }
            }
        }
        if (queue.size() >= maxBatchSize) {
            // Drain asynchronously so the caller never blocks on downstream I/O.
            try {
                scheduleDrain();
            } catch (RejectedExecutionException rex) {
                // Executor shut down after the closed-check; the batch is enqueued but unreachable.
                return CompletableFuture.completedFuture(
                        ConsumeResult.retryableRejected("batcher shutting down"));
            }
        }
        return ConsumeResult.acceptedStage();
    }

    /// Number of dropped batches (not telemetry items). Non-decreasing over the batcher's lifetime.
    public long droppedCount() {
        return drops.sum();
    }

    /// Number of batches currently buffered.
    public int queued() {
        return queue.size();
    }

    @Override
    public CompletionStage<Void> forceFlush(Duration timeout) {
        // Drain and await real downstream completion, bounded by `timeout`, propagating failures.
        return enqueueDrain().orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /// Stops the timer and drains downstream. Completes only when the final drain's delivery
    /// finishes; propagates timeout/failure/rejection instead of a false success.
    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        timer.cancel(false);
        // Capture drainTail under the same lock scheduleDrain uses, so any
        // drain ordered before shutdown is included and no new one can start.
        final CompletableFuture<Void> drain;
        synchronized (drainMutex) {
            drain = drainTail.thenComposeAsync(ignored -> drainOnce(), drainExecutor);
        }
        return drain
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS)
                .whenComplete((ignored, t) -> {
                    drainExecutor.shutdown();
                    if (ownsScheduler) {
                        scheduler.shutdown();
                    }
                });
    }

    @Override
    public void close() {
        // Best-effort: await the drain but don't throw out of close(); use shutdown() to observe.
        try {
            shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        } catch (CompletionException | CancellationException e) {
            log.warn("batcher did not cleanly drain on close: {}",
                    e.getCause() != null ? e.getCause().toString() : e.toString());
        }
    }

    private void flushIfDue() {
        try {
            scheduleDrain();
        } catch (RejectedExecutionException rex) {
            // Timer tick raced shutdown teardown; nothing to drain.
        }
    }

    /// Fire-and-forget drain (size/timer); logs failures since nobody awaits it.
    private void scheduleDrain() {
        // Once shutdown starts, no new drains may be scheduled.
        if (closed.get()) {
            return;
        }
        CompletableFuture<Void> drain;
        synchronized (drainMutex) {
            if (closed.get()) {
                return;
            }
            drain = drainTail.thenComposeAsync(ignored -> drainOnce(), drainExecutor);
            // Error-isolated so a failed drain doesn't block the next.
            drainTail = drain.exceptionally(t -> null);
        }
        drain.whenComplete((ignored, t) -> {
            if (t != null) {
                log.warn("flush failed", t);
            }
        });
    }

    /// Enqueues a serialized drain; the returned future completes when this drain's delivery does.
    private CompletableFuture<Void> enqueueDrain() {
        synchronized (drainMutex) {
            var drain = drainTail.thenComposeAsync(ignored -> drainOnce(), drainExecutor);
            // Error-isolated so a failed drain doesn't block the next.
            drainTail = drain.exceptionally(t -> null);
            return drain;
        }
    }

    private CompletableFuture<Void> drainOnce() {
        var snapshot = new ArrayList<T>(queue.size());
        queue.drainTo(snapshot);
        if (snapshot.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            var merged = merger.merge(snapshot);
            return downstream.consume(merged).toCompletableFuture()
                    .thenCompose(BatchingProcessor::asDeliveryOutcome);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /// Maps a downstream result to success/failure so a `Partial`/`Rejected` is surfaced, not discarded.
    private static CompletableFuture<Void> asDeliveryOutcome(ConsumeResult<?> result) {
        return switch (result) {
            case ConsumeResult.Accepted<?> _ -> CompletableFuture.completedFuture(null);
            case ConsumeResult.Partial<?> p -> CompletableFuture.failedFuture(new BatchDeliveryException(
                    "downstream partially rejected " + p.rejectedItems() + " item(s): " + p.message()));
            case ConsumeResult.Rejected<?> r -> CompletableFuture.failedFuture(
                    new BatchDeliveryException("downstream rejected batch: " + r.message(), r.cause()));
        };
    }

    /// A batch could not be fully delivered downstream (threw, rejected, or partially rejected).
    /// Surfaced from [#shutdown] and [#forceFlush] so a failed flush is observable.
    public static final class BatchDeliveryException extends RuntimeException {
        BatchDeliveryException(String message) {
            super(message);
        }

        BatchDeliveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /// Opens a builder for a [BatchingProcessor] over [TraceData].
    public static Builder<TraceData> forTraces() {
        return new Builder<>(BatchingProcessor::mergeTraces, TraceData::spanCount);
    }

    /// Opens a builder for a [BatchingProcessor] over [MetricsData].
    public static Builder<MetricsData> forMetrics() {
        return new Builder<>(BatchingProcessor::mergeMetrics, BatchingProcessor::countDataPoints);
    }

    /// Opens a builder for a [BatchingProcessor] over [LogsData].
    public static Builder<LogsData> forLogs() {
        return new Builder<>(BatchingProcessor::mergeLogs, LogsData::logRecordCount);
    }

    /// Opens a builder for a [BatchingProcessor] over [ProfilesData].
    public static Builder<ProfilesData> forProfiles() {
        return new Builder<>(BatchingProcessor::mergeProfiles, ProfilesData::profileCount);
    }

    @FunctionalInterface
    private interface Merger<T> {
        T merge(List<T> snapshot);
    }

    /// Metrics report rejected *data points* (nested in each metric's data kind), so this walks the
    /// [Metric.Data] variants rather than counting [Metric] objects.
    private static long countDataPoints(MetricsData metrics) {
        var count = 0L;
        for (var resource : metrics.resourceMetrics()) {
            for (var scope : resource.scopeMetrics()) {
                for (var metric : scope.metrics()) {
                    count += switch (metric.data()) {
                        case Metric.NoData _ -> 0L;
                        case Metric.Gauge g -> g.points().size();
                        case Metric.Sum s -> s.points().size();
                        case Metric.Histogram h -> h.points().size();
                        case Metric.ExponentialHistogram e -> e.points().size();
                        case Metric.Summary s -> s.points().size();
                    };
                }
            }
        }
        return count;
    }

    private static TraceData mergeTraces(List<TraceData> snapshot) {
        var combined = new ArrayList<TraceData.ResourceSpans>();
        snapshot.forEach(t -> combined.addAll(t.resourceSpans()));
        return new TraceData(combined);
    }

    private static MetricsData mergeMetrics(List<MetricsData> snapshot) {
        var combined = new ArrayList<MetricsData.ResourceMetrics>();
        snapshot.forEach(m -> combined.addAll(m.resourceMetrics()));
        return new MetricsData(combined);
    }

    private static LogsData mergeLogs(List<LogsData> snapshot) {
        var combined = new ArrayList<LogsData.ResourceLogs>();
        snapshot.forEach(l -> combined.addAll(l.resourceLogs()));
        return new LogsData(combined);
    }

    /// Profiles carry an opaque, batch-level `ProfilesDictionary`; each profile's payload references
    /// it by index. Merging is only lossless when the batches agree on that dictionary: the
    /// resource-profiles are concatenated under the one shared (or only non-empty) dictionary.
    /// Distinct non-empty dictionaries cannot be merged without re-indexing every reference — which
    /// the passthrough model deliberately avoids — so this fails loudly (surfaced as a failed flush)
    /// rather than emitting an index-corrupted batch.
    private static ProfilesData mergeProfiles(List<ProfilesData> snapshot) {
        var combined = new ArrayList<ProfilesData.ResourceProfiles>();
        var dictionary = new byte[0];
        for (var p : snapshot) {
            combined.addAll(p.resourceProfiles());
            var dict = p.dictionary();
            if (dict.length == 0) {
                continue;
            }
            if (dictionary.length == 0) {
                dictionary = dict;
            } else if (!Arrays.equals(dictionary, dict)) {
                throw new IllegalStateException(
                        "cannot batch-merge profiles carrying distinct ProfilesDictionaries without "
                                + "re-indexing; forward profiles 1:1 or disable profiles batching");
            }
        }
        return new ProfilesData(combined, dictionary);
    }

    /// Builder for [BatchingProcessor].
    public static final class Builder<T> {

        private final Merger<T> merger;
        private final ToLongFunction<T> itemCounter;
        private @Nullable Sink<? super T> downstream;
        private int maxBatchSize = 512;
        private int queueCapacity = 2048;
        private DropPolicy dropPolicy = DropPolicy.DROP_NEWEST;
        private @Nullable LongAdder drops;
        private @Nullable ScheduledExecutorService scheduler;

        private Builder(Merger<T> merger, ToLongFunction<T> itemCounter) {
            this.merger = merger;
            this.itemCounter = itemCounter;
        }

        /// The downstream consumer the flushed batch is forwarded to. Required.
        public Builder<T> downstream(Sink<? super T> downstream) {
            this.downstream = downstream;
            return this;
        }

        /// The size threshold at which the batcher flushes immediately on `consume`.
        public Builder<T> maxBatchSize(int items) {
            if (items < 1) throw new IllegalArgumentException("maxBatchSize must be >= 1");
            this.maxBatchSize = items;
            return this;
        }

        /// The capacity of the bounded ingest queue.
        public Builder<T> queueCapacity(int items) {
            if (items < 1) throw new IllegalArgumentException("queueCapacity must be >= 1");
            this.queueCapacity = items;
            return this;
        }

        /// What happens when the ingest queue is full. Defaults to [DropPolicy#DROP_NEWEST].
        public Builder<T> dropPolicy(DropPolicy policy) {
            this.dropPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        /// Optional external counter receiving drop events.
        public Builder<T> dropCounter(LongAdder drops) {
            this.drops = drops;
            return this;
        }

        /// Use a caller-supplied scheduler for timer-driven flushes. By default the batcher owns
        /// a single-thread daemon scheduler.
        public Builder<T> scheduler(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public BatchingProcessor<T> build() {
            if (downstream == null) {
                throw new IllegalStateException("downstream must be set");
            }
            return new BatchingProcessor<>(this);
        }
    }
}
