package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Consumer;
import dev.nthings.otlp4j.pipeline.Pipeline;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// An asynchronous, queue-backed, timer-triggered batching processor.
///
/// Flushes downstream when either the size threshold or `maxBatchAge` is reached. Implements
/// [Pipeline.Flushable] and is closed via [#shutdown].
///
/// @param <T> the OTLP signal carried by this batcher
public final class BatchingProcessor<T> implements Consumer<T>, Pipeline.Flushable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BatchingProcessor.class);

    private final Consumer<? super T> downstream;
    private final int maxBatchSize;
    private final Duration maxBatchAge;
    private final ArrayBlockingQueue<T> queue;
    private final DropPolicy dropPolicy;
    private final LongAdder drops;
    private final ScheduledExecutorService scheduler;
    private final boolean ownsScheduler;
    private final ScheduledFuture<?> timer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Semaphore flushLock = new Semaphore(1);
    private final Merger<T> merger;

    private BatchingProcessor(Builder<T> b) {
        this.downstream = Objects.requireNonNull(b.downstream, "downstream");
        this.maxBatchSize = b.maxBatchSize;
        this.maxBatchAge = b.maxBatchAge;
        this.queue = new ArrayBlockingQueue<>(b.queueCapacity);
        this.dropPolicy = b.dropPolicy;
        this.drops = b.drops == null ? new LongAdder() : b.drops;
        this.merger = b.merger;
        this.ownsScheduler = b.scheduler == null;
        this.scheduler = b.scheduler == null
                ? Executors.newSingleThreadScheduledExecutor(r -> {
                    var t = new Thread(r, "otlp4j-batcher");
                    t.setDaemon(true);
                    return t;
                })
                : b.scheduler;
        long ageNanos = b.maxBatchAge.toNanos();
        this.timer = scheduler.scheduleAtFixedRate(this::flushIfDue, ageNanos, ageNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public CompletionStage<ConsumeResult<T>> consume(T batch) {
        if (closed.get()) {
            return CompletableFuture.completedFuture(ConsumeResult.rejected("batcher closed"));
        }
        boolean offered = queue.offer(batch);
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
                    return CompletableFuture.completedFuture(
                            ConsumeResult.partial(1, "batcher queue full"));
                }
                case BLOCK -> {
                    try {
                        queue.put(batch);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return CompletableFuture.completedFuture(
                                ConsumeResult.rejected("interrupted waiting for batcher capacity", e));
                    }
                }
                case ERROR -> {
                    drops.increment();
                    return CompletableFuture.completedFuture(
                            ConsumeResult.rejected("batcher queue full"));
                }
            }
        }
        if (queue.size() >= maxBatchSize) {
            // Run the drain on our scheduler so the caller never blocks on downstream I/O.
            try {
                scheduler.execute(this::flushNow);
            } catch (RejectedExecutionException rex) {
                // The scheduler shut down between consume()'s closed-check and now; the batch
                // is already enqueued but no longer reachable.
                return CompletableFuture.completedFuture(
                        ConsumeResult.rejected("batcher shutting down"));
            }
        }
        return ConsumeResult.acceptedStage();
    }

    /// Item-count of currently dropped batches. Non-decreasing over the lifetime of the batcher.
    public long droppedCount() {
        return drops.sum();
    }

    /// Number of batches currently buffered.
    public int queued() {
        return queue.size();
    }

    @Override
    public CompletionStage<Void> forceFlush(Duration timeout) {
        return flushNow();
    }

    /// Drains buffered batches downstream and stops the timer; completes on drain or `timeout`.
    public CompletionStage<Void> shutdown(Duration timeout) {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        timer.cancel(false);
        var drained = flushNow()
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS)
                .exceptionally(t -> null);
        if (ownsScheduler) {
            drained = drained.whenComplete((_, _) -> scheduler.shutdown()).toCompletableFuture();
        }
        return drained;
    }

    @Override
    public void close() {
        shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
    }

    private void flushIfDue() {
        flushNow().whenComplete((_, t) -> {
            if (t != null) {
                log.warn("scheduled flush failed", t);
            }
        });
    }

    private CompletableFuture<Void> flushNow() {
        // Coalesce concurrent flushes — drain whatever is queued under the lock.
        if (!flushLock.tryAcquire()) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            var snapshot = new ArrayList<T>(queue.size());
            queue.drainTo(snapshot);
            if (snapshot.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            var merged = merger.merge(snapshot);
            try {
                return downstream.consume(merged).toCompletableFuture().thenApply(r -> null);
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        } finally {
            flushLock.release();
        }
    }

    /// Opens a builder for a [BatchingProcessor] over [TraceData].
    public static Builder<TraceData> forTraces() {
        return new Builder<>(BatchingProcessor::mergeTraces);
    }

    /// Opens a builder for a [BatchingProcessor] over [MetricsData].
    public static Builder<MetricsData> forMetrics() {
        return new Builder<>(BatchingProcessor::mergeMetrics);
    }

    /// Opens a builder for a [BatchingProcessor] over [LogsData].
    public static Builder<LogsData> forLogs() {
        return new Builder<>(BatchingProcessor::mergeLogs);
    }

    /// Opens a builder for a [BatchingProcessor] over [ProfilesData].
    public static Builder<ProfilesData> forProfiles() {
        return new Builder<>(BatchingProcessor::mergeProfiles);
    }

    @FunctionalInterface
    private interface Merger<T> {
        T merge(List<T> snapshot);
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

    private static ProfilesData mergeProfiles(List<ProfilesData> snapshot) {
        var combined = new ArrayList<ProfilesData.ResourceProfiles>();
        snapshot.forEach(p -> combined.addAll(p.resourceProfiles()));
        return new ProfilesData(combined);
    }

    /// Builder for [BatchingProcessor].
    public static final class Builder<T> {

        private final Merger<T> merger;
        private Consumer<? super T> downstream;
        private int maxBatchSize = 512;
        private Duration maxBatchAge = Duration.ofSeconds(5);
        private int queueCapacity = 2048;
        private DropPolicy dropPolicy = DropPolicy.DROP_NEWEST;
        private LongAdder drops;
        private ScheduledExecutorService scheduler;

        private Builder(Merger<T> merger) {
            this.merger = merger;
        }

        /// The downstream consumer the flushed batch is forwarded to. Required.
        public Builder<T> downstream(Consumer<? super T> downstream) {
            this.downstream = downstream;
            return this;
        }

        /// The size threshold at which the batcher flushes immediately on `consume`.
        public Builder<T> maxBatchSize(int items) {
            if (items < 1) throw new IllegalArgumentException("maxBatchSize must be >= 1");
            this.maxBatchSize = items;
            return this;
        }

        /// The age threshold at which a partially filled queue is flushed by the timer.
        public Builder<T> maxBatchAge(Duration age) {
            if (age.isZero() || age.isNegative()) throw new IllegalArgumentException("maxBatchAge must be > 0");
            this.maxBatchAge = age;
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
