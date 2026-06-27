package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.core.Sink;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Queue-backed serial drain engine shared by [BatchingProcessor].
final class BatchDrainEngine<T> {

    private static final Logger log = LoggerFactory.getLogger(BatchDrainEngine.class);

    private final ArrayBlockingQueue<T> queue;
    private final Sink<? super T> downstream;
    private final Function<List<T>, T> merger;
    private final AtomicBoolean closed;
    private final ScheduledFuture<?> timer;
    private final ExecutorService drainExecutor;
    private final Object drainMutex = new Object();
    private CompletableFuture<Void> drainTail = CompletableFuture.completedFuture(null);

    BatchDrainEngine(
            ArrayBlockingQueue<T> queue,
            Sink<? super T> downstream,
            Function<List<T>, T> merger,
            AtomicBoolean closed,
            ScheduledExecutorService scheduler,
            ExecutorService drainExecutor,
            Runnable flushIfDue) {
        this.queue = queue;
        this.downstream = downstream;
        this.merger = merger;
        this.closed = closed;
        this.drainExecutor = drainExecutor;
        this.timer = scheduler.scheduleAtFixedRate(flushIfDue, 1, 1, TimeUnit.SECONDS);
    }

    void cancelTimer() {
        timer.cancel(false);
    }

    void scheduleDrain() {
        if (closed.get()) {
            return;
        }
        CompletableFuture<Void> drain;
        synchronized (drainMutex) {
            if (closed.get()) {
                return;
            }
            drain = drainTail.thenComposeAsync(ignored -> drainOnce(), drainExecutor);
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
        var snapshot = new ArrayList<T>(queue.size());
        queue.drainTo(snapshot);
        if (snapshot.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            var merged = merger.apply(snapshot);
            return downstream.consume(merged).toCompletableFuture()
                    .thenCompose(BatchDrainEngine::asDeliveryOutcome);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    static CompletableFuture<Void> asDeliveryOutcome(ConsumeResult<?> result) {
        return switch (result) {
            case ConsumeResult.Accepted<?> _ -> CompletableFuture.completedFuture(null);
            case ConsumeResult.Partial<?> p ->
                CompletableFuture.failedFuture(new BatchingProcessor.BatchDeliveryException(
                        "downstream partially rejected " + p.rejectedItems() + " item(s): " + p.message()));
            case ConsumeResult.Rejected<?> r -> CompletableFuture.failedFuture(
                    new BatchingProcessor.BatchDeliveryException("downstream rejected batch: " + r.message(),
                            r.cause()));
        };
    }

    void tryScheduleDrain() throws RejectedExecutionException {
        scheduleDrain();
    }
}
