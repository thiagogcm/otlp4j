package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.ops.internal.Signal;
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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// An asynchronous, queue-backed, timer-triggered batching processor.
///
/// > **Warning:** enqueueing acknowledges immediately. `consume` returns `Accepted` once the
/// > batch is queued - before it is delivered downstream. A timer- or threshold-triggered drain
/// > that fails downstream is logged at WARN and the drained batches are **lost with no upstream
/// > signal**; only [#forceFlush] and [#shutdown] surface a [BatchDeliveryException]. Enable
/// > [Builder#reflectDownstreamFailures] to convert sustained downstream failure into retryable
/// > ingress rejections carrying a retry-after hint, so senders back off and retain their data.
///
/// Threading: one JVM-wide platform daemon thread (`otlp4j-batch-timer`) drives every batcher's
/// flush timer; each instance adds only a single cheap virtual thread for its serial drain.
public final class Batcher<T> implements Sink<T>, Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Batcher.class);

    /// The fixed drain-timer interval. Also used as the retry-after hint on a [OverflowPolicy#FAIL]
    /// overflow rejection: a full queue is drained within one timer tick, so retrying after one
    /// interval is the earliest attempt that can succeed.
    static final Duration FLUSH_INTERVAL = Duration.ofSeconds(1);

    private final Signal signal;
    private final Sink<? super T> downstream;
    private final int flushThreshold;
    private final ArrayBlockingQueue<T> queue;
    private final OverflowPolicy overflowPolicy;
    private final boolean reflectDownstreamFailures;
    private final LongAdder drops = new LongAdder();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService drainExecutor;
    private final BatchDrainEngine<T> drainEngine;

    /// Degraded window: a failed drain sets it one flush interval into the future; a successful
    /// drain clears it. While the window is open and [Builder#reflectDownstreamFailures] is on,
    /// `consume` rejects retryably instead of enqueueing. The window expiring by itself admits a
    /// probe batch, so a healed downstream recovers without a stuck state.
    private volatile long degradedUntilNanos;
    private volatile @Nullable Throwable lastDrainFailure;

    /// One JVM-wide platform daemon thread drives every batcher's flush timer. Never shut down;
    /// instances stay independent through per-instance timer cancellation
    /// (`setRemoveOnCancelPolicy` keeps cancelled tasks out of the shared queue).
    private static final class SharedTimer {

        static final ScheduledExecutorService INSTANCE = create();

        private static ScheduledExecutorService create() {
            var timer = new ScheduledThreadPoolExecutor(1,
                    Thread.ofPlatform().daemon().name("otlp4j-batch-timer").factory());
            timer.setRemoveOnCancelPolicy(true);
            return timer;
        }
    }

    private Batcher(Signal signal, Builder<T> b) {
        this.signal = signal;
        this.downstream = Objects.requireNonNull(b.downstream, "downstream");
        this.flushThreshold = b.flushThreshold;
        this.queue = new ArrayBlockingQueue<>(b.queueCapacity);
        this.overflowPolicy = b.overflowPolicy;
        this.reflectDownstreamFailures = b.reflectDownstreamFailures;
        this.drainExecutor = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("otlp4j-batcher-drain").factory());
        this.drainEngine = new BatchDrainEngine<>(
                queue, downstream, signal::merge, closed, SharedTimer.INSTANCE, drainExecutor,
                this::flushIfDue, this::onDrainOutcome);
    }

    /// Tracks downstream health from drain outcomes.
    private void onDrainOutcome(@Nullable Throwable failure) {
        if (failure == null) {
            degradedUntilNanos = 0L;
            lastDrainFailure = null;
        } else {
            lastDrainFailure = failure;
            degradedUntilNanos = System.nanoTime() + FLUSH_INTERVAL.toNanos();
        }
    }

    @Override
    public CompletionStage<ConsumeResult> consume(T batch) {
        if (closed.get()) {
            return CompletableFuture.completedFuture(ConsumeResult.retryable("batcher closed"));
        }
        // 0 is the healthy sentinel (nanoTime has an arbitrary origin, so compare relatively).
        var degradedUntil = degradedUntilNanos;
        if (reflectDownstreamFailures && degradedUntil != 0L && System.nanoTime() - degradedUntil < 0) {
            return CompletableFuture.completedFuture(ConsumeResult.throttled(
                    FLUSH_INTERVAL, "batcher degraded: last flush failed", lastDrainFailure));
        }
        var offered = queue.offer(batch);
        if (!offered) {
            switch (overflowPolicy) {
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
                                ConsumeResult.permanent("interrupted waiting for batcher capacity", e));
                    }
                }
                case FAIL -> {
                    drops.increment();
                    return CompletableFuture.completedFuture(
                            ConsumeResult.throttled(FLUSH_INTERVAL, "batcher queue full"));
                }
            }
        }
        if (queue.size() >= flushThreshold) {
            try {
                drainEngine.tryScheduleDrain();
            } catch (RejectedExecutionException rex) {
                return CompletableFuture.completedFuture(
                        ConsumeResult.retryable("batcher shutting down"));
            }
        }
        // Shutdown may have flipped closed after the initial check; reclaim the batch to avoid stranding it.
        if (closed.get() && queue.remove(batch)) {
            return CompletableFuture.completedFuture(ConsumeResult.retryable("batcher closed"));
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
                .whenComplete((ignored, t) -> drainExecutor.shutdown());
    }

    @Override
    public void close() {
        try {
            shutdown(Lifecycle.DEFAULT_GRACE_PERIOD).toCompletableFuture().join();
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

    /// Surfaces a flush failure - an impossible merge or a downstream rejection - from
    /// [#shutdown] and [#forceFlush].
    public static final class BatchDeliveryException extends RuntimeException {
        BatchDeliveryException(String message) {
            super(message);
        }

        BatchDeliveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static Builder<TracesData> forTraces() {
        return new Builder<>(Signal.TRACES);
    }

    public static Builder<MetricsData> forMetrics() {
        return new Builder<>(Signal.METRICS);
    }

    public static Builder<LogsData> forLogs() {
        return new Builder<>(Signal.LOGS);
    }

    /// Profiles batching. Only safe when every merged batch shares the same `ProfilesDictionary`;
    /// otherwise flush fails with [BatchDeliveryException].
    public static Builder<ProfilesData> forProfilesUnsafe() {
        return new Builder<>(Signal.PROFILES);
    }

    public static final class Builder<T> {
        private final Signal signal;
        private @Nullable Sink<? super T> downstream;
        private int flushThreshold = 512;
        private int queueCapacity = 2048;
        private OverflowPolicy overflowPolicy = OverflowPolicy.DROP_NEWEST;
        private boolean reflectDownstreamFailures = false;

        private Builder(Signal signal) {
            this.signal = signal;
        }

        public Builder<T> downstream(Sink<? super T> downstream) {
            this.downstream = downstream;
            return this;
        }

        /// Flushes the queue once it holds this many batches (the drain trigger).
        public Builder<T> flushThreshold(int batches) {
            if (batches < 1)
                throw new IllegalArgumentException("flushThreshold must be >= 1");
            this.flushThreshold = batches;
            return this;
        }

        /// Caps the queue at this many batches (the hard limit; overflow follows [#overflowPolicy]).
        public Builder<T> queueCapacity(int batches) {
            if (batches < 1)
                throw new IllegalArgumentException("queueCapacity must be >= 1");
            this.queueCapacity = batches;
            return this;
        }

        public Builder<T> overflowPolicy(OverflowPolicy policy) {
            this.overflowPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        /// Reflects sustained downstream failure back at ingress (default off). After a drain
        /// fails, `consume` rejects retryably with a one-flush-interval retry-after hint - the
        /// batch is NOT enqueued, so the sender retains it - until the window expires (admitting
        /// a probe batch) or a drain succeeds. Off, the batcher is fire-and-forget: failed
        /// timer/threshold drains are logged and their batches lost.
        public Builder<T> reflectDownstreamFailures(boolean reflect) {
            this.reflectDownstreamFailures = reflect;
            return this;
        }

        public Batcher<T> build() {
            if (downstream == null) {
                throw new IllegalStateException("downstream must be set");
            }
            return new Batcher<>(signal, this);
        }
    }
}
