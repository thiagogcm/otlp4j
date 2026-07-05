package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Sink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Queue-backed serial drain engine shared by [Batcher].
final class BatchDrainEngine<T> {

    private static final Logger log = LoggerFactory.getLogger(BatchDrainEngine.class);

    /// Observes the outcome of every non-empty drain (an empty drain proves nothing about
    /// downstream health). Backs the batcher's degraded-state reflection and drain-loss counter.
    @FunctionalInterface
    interface DrainListener {

        /// Called once per non-empty drain; `failure` is null on success. `batchCount` is the
        /// number of queued batches the drain carried (feeds the drain-loss counter on failure).
        void onDrainOutcome(@Nullable Throwable failure, int batchCount);
    }

    private final ArrayBlockingQueue<QueueEntry<T>> queue;
    private final Supplier<@Nullable Sink<? super T>> target;
    private final Function<List<T>, T> merger;
    private final AtomicBoolean closed;
    private final ScheduledFuture<?> timer;
    private final ExecutorService drainExecutor;
    private final DrainListener listener;
    private final @Nullable Duration deliveryTimeout;
    private final Object drainMutex = new Object();
    private final AtomicBoolean drainPending = new AtomicBoolean();
    private CompletableFuture<Void> drainTail = CompletableFuture.completedFuture(null);

    BatchDrainEngine(
            ArrayBlockingQueue<QueueEntry<T>> queue,
            Supplier<@Nullable Sink<? super T>> target,
            Function<List<T>, T> merger,
            AtomicBoolean closed,
            ScheduledExecutorService scheduler,
            ExecutorService drainExecutor,
            Runnable flushIfDue,
            DrainListener listener,
            Duration flushInterval,
            @Nullable Duration deliveryTimeout) {
        this.queue = queue;
        this.target = target;
        this.merger = merger;
        this.closed = closed;
        this.drainExecutor = drainExecutor;
        this.listener = listener;
        this.deliveryTimeout = deliveryTimeout;
        var intervalMillis = flushInterval.toMillis();
        this.timer = scheduler.scheduleAtFixedRate(
                flushIfDue, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    void cancelTimer() {
        timer.cancel(false);
    }

    void scheduleDrain() {
        if (closed.get()) {
            return;
        }
        // Coalesce: keep at most one drain queued behind the in-flight one, preventing unbounded
        // chain growth during outages. The flag clears once the drain starts.
        if (!drainPending.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture<Void> drain;
        synchronized (drainMutex) {
            if (closed.get()) {
                drainPending.set(false);
                return;
            }
            drain = drainTail.thenComposeAsync(ignored -> {
                drainPending.set(false);
                return drainOnce();
            }, drainExecutor);
            drainTail = drain.exceptionally(t -> null);
        }
        drain.whenComplete((ignored, t) -> {
            if (t != null) {
                log.warn("flush failed", t);
            }
        });
    }

    CompletableFuture<Void> enqueueDrain() {
        synchronized (drainMutex) {
            var drain = drainTail.thenComposeAsync(ignored -> drainOnce(), drainExecutor);
            drainTail = drain.exceptionally(t -> null);
            return drain;
        }
    }

    CompletableFuture<Void> finalDrain() {
        synchronized (drainMutex) {
            return drainTail.thenComposeAsync(ignored -> drainOnce(), drainExecutor);
        }
    }

    private CompletableFuture<Void> drainOnce() {
        var entries = new ArrayList<QueueEntry<T>>(queue.size());
        queue.drainTo(entries);
        if (entries.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return deliver(entries)
                .whenComplete((ignored, failure) -> listener.onDrainOutcome(failure, entries.size()));
    }

    private CompletableFuture<Void> deliver(List<QueueEntry<T>> entries) {
        var downstream = target.get();
        if (downstream == null) {
            // The queue is empty until process/consume has bound next, so this is a safety net,
            // never hit in practice (see the concurrency argument in the F2 design).
            fanAcks(entries, ConsumeResult.retryable("batcher has no downstream"));
            return CompletableFuture.completedFuture(null);
        }
        T merged;
        try {
            merged = merger.apply(batchesOf(entries));
        } catch (RuntimeException e) {
            // An impossible merge surfaces as [BatchDeliveryException], same as a downstream rejection.
            fanAcks(entries, ConsumeResult.permanentFrom("batcher merge failed", e));
            return CompletableFuture.failedFuture(new Batcher.BatchDeliveryException(
                    "failed to flush drained batch: " + e.getMessage(), e));
        }
        CompletableFuture<ConsumeResult> stage;
        try {
            stage = Objects.requireNonNull(
                    downstream.consume(merged), "downstream returned a null stage").toCompletableFuture();
        } catch (Throwable e) {
            // Downstream threw synchronously: ack callers retryably, Void outcome keeps the raw cause.
            fanAcks(entries, ConsumeResult.retryable("batcher downstream threw", e));
            return CompletableFuture.failedFuture(e);
        }
        if (deliveryTimeout != null) {
            stage = stage.orTimeout(deliveryTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        return stage.handle((result, failure) -> {
            if (failure != null) {
                var cause = unwrap(failure);
                fanAcks(entries, ConsumeResult.retryable("batcher downstream delivery failed", cause));
                return CompletableFuture.<Void>failedFuture(cause);
            }
            fanAcks(entries, result);
            return asDeliveryOutcome(result);
        }).thenCompose(f -> f);
    }

    /// Maps each entry to its batch for the merger.
    private static <T> List<T> batchesOf(List<QueueEntry<T>> entries) {
        var batches = new ArrayList<T>(entries.size());
        for (var entry : entries) {
            batches.add(entry.batch);
        }
        return batches;
    }

    /// Completes every non-null ack with `result` (a no-op in [AckMode#ON_ENQUEUE], where all acks
    /// are null). `complete` is at-most-once, guarding double-completion races.
    private static <T> void fanAcks(List<QueueEntry<T>> entries, ConsumeResult result) {
        for (var entry : entries) {
            if (entry.ack != null) {
                entry.ack.complete(result);
            }
        }
    }

    /// Unwraps a [CompletionException] to its cause, following the API-wide convention. The
    /// package-private `PipelineLifecycle.unwrap` lives in a sibling package this one cannot reach.
    private static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException ce && ce.getCause() != null ? ce.getCause() : t;
    }

    static CompletableFuture<Void> asDeliveryOutcome(ConsumeResult result) {
        return switch (result) {
            case ConsumeResult.Accepted _ -> CompletableFuture.completedFuture(null);
            case ConsumeResult.Partial p ->
                CompletableFuture.failedFuture(new Batcher.BatchDeliveryException(
                        "downstream partially rejected " + p.rejectedItems() + " item(s): " + p.message()));
            case ConsumeResult.Rejected r -> CompletableFuture.failedFuture(
                    new Batcher.BatchDeliveryException("downstream rejected batch: " + r.message(),
                            r.cause()));
        };
    }

    void tryScheduleDrain() throws RejectedExecutionException {
        scheduleDrain();
    }
}
