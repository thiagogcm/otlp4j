package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.Flushable;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.model.ConsumeResult;
import java.time.Duration;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// An asynchronous, queue-backed, timer-triggered batching processor.
public final class BatchingProcessor<T> implements Sink<T>, Drainable, Flushable {

    private static final Logger log = LoggerFactory.getLogger(BatchingProcessor.class);

    private final Signal signal;
    private final Sink<? super T> downstream;
    private final int maxBatchSize;
    private final ArrayBlockingQueue<T> queue;
    private final DropPolicy dropPolicy;
    private final LongAdder drops;
    private final ScheduledExecutorService scheduler;
    private final boolean ownsScheduler;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService drainExecutor;
    private final BatchDrainEngine<T> drainEngine;

    private BatchingProcessor(Signal signal, Builder<T> b) {
        this.signal = signal;
        this.downstream = Objects.requireNonNull(b.downstream, "downstream");
        this.maxBatchSize = b.maxBatchSize;
        this.queue = new ArrayBlockingQueue<>(b.queueCapacity);
        this.dropPolicy = b.dropPolicy;
        this.drops = b.drops == null ? new LongAdder() : b.drops;
        this.ownsScheduler = b.scheduler == null;
        this.scheduler = b.scheduler == null
                ? Executors.newSingleThreadScheduledExecutor(
                        Thread.ofPlatform().daemon().name("otlp4j-batcher").factory())
                : b.scheduler;
        this.drainExecutor = Executors.newSingleThreadExecutor(
                Thread.ofPlatform().daemon().name("otlp4j-batcher-drain").factory());
        this.drainEngine = new BatchDrainEngine<>(
                queue, downstream, signal::merge, closed, scheduler, drainExecutor, this::flushIfDue);
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
                    var rejectedItems = signal.itemCount(batch);
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
            try {
                drainEngine.tryScheduleDrain();
            } catch (RejectedExecutionException rex) {
                return CompletableFuture.completedFuture(
                        ConsumeResult.retryableRejected("batcher shutting down"));
            }
        }
        // Close/consume race: shutdown may have flipped `closed` and taken the final-drain
        // snapshot after our initial check above. If so, reclaim this batch and reject it
        // rather than leaving it stranded in the queue with no further drain to deliver it.
        if (closed.get() && queue.remove(batch)) {
            return CompletableFuture.completedFuture(ConsumeResult.retryableRejected("batcher closed"));
        }
        return ConsumeResult.acceptedStage();
    }

    public long droppedCount() {
        return drops.sum();
    }

    public int queued() {
        return queue.size();
    }

    @Override
    public CompletionStage<Void> forceFlush(Duration timeout) {
        return drainEngine.enqueueDrain().orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        drainEngine.cancelTimer();
        return drainEngine.finalDrain()
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
        try {
            shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        } catch (CompletionException | CancellationException e) {
            log.warn("batcher did not cleanly drain on close: {}",
                    e.getCause() != null ? e.getCause().toString() : e.toString());
        }
    }

    private void flushIfDue() {
        try {
            drainEngine.scheduleDrain();
        } catch (RejectedExecutionException ignored) {
            // Timer tick raced shutdown teardown.
        }
    }

    /// Surfaces a flush failure — an impossible merge or a downstream rejection — from
    /// [#shutdown] and [#forceFlush].
    public static final class BatchDeliveryException extends RuntimeException {
        BatchDeliveryException(String message) {
            super(message);
        }

        BatchDeliveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static Builder<TraceData> forTraces() {
        return new Builder<>(Signal.TRACES);
    }

    public static Builder<MetricsData> forMetrics() {
        return new Builder<>(Signal.METRICS);
    }

    public static Builder<LogsData> forLogs() {
        return new Builder<>(Signal.LOGS);
    }

    /// Experimental profiles batching. Only safe when every merged batch shares the
    /// same `ProfilesDictionary`; otherwise flush fails
    /// with [BatchDeliveryException].
    public static Builder<ProfilesData> forProfilesUnsafe() {
        return new Builder<>(Signal.PROFILES);
    }

    public static final class Builder<T> {
        private final Signal signal;
        private @Nullable Sink<? super T> downstream;
        private int maxBatchSize = 512;
        private int queueCapacity = 2048;
        private DropPolicy dropPolicy = DropPolicy.DROP_NEWEST;
        private @Nullable LongAdder drops;
        private @Nullable ScheduledExecutorService scheduler;

        private Builder(Signal signal) {
            this.signal = signal;
        }

        public Builder<T> downstream(Sink<? super T> downstream) {
            this.downstream = downstream;
            return this;
        }

        public Builder<T> maxBatchSize(int items) {
            if (items < 1)
                throw new IllegalArgumentException("maxBatchSize must be >= 1");
            this.maxBatchSize = items;
            return this;
        }

        public Builder<T> queueCapacity(int items) {
            if (items < 1)
                throw new IllegalArgumentException("queueCapacity must be >= 1");
            this.queueCapacity = items;
            return this;
        }

        public Builder<T> dropPolicy(DropPolicy policy) {
            this.dropPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        public Builder<T> dropCounter(LongAdder drops) {
            this.drops = drops;
            return this;
        }

        public Builder<T> scheduler(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public BatchingProcessor<T> build() {
            if (downstream == null) {
                throw new IllegalStateException("downstream must be set");
            }
            return new BatchingProcessor<>(signal, this);
        }
    }
}
