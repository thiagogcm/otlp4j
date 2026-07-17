package dev.nthings.otlp4j.components.internal;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The batch processor's bounded delivery boundary: the single queueing point
/// [dev.nthings.otlp4j.components.batch.BatchProcessor] flushes each merged-and-split unit into,
/// decoupling the flush from the downstream `consume`. It is a bounded
/// [ArrayBlockingQueue] of `queueSize` entries, drained by `numConsumers` worker virtual threads that
/// each call the downstream `consume`. It carries two independent axes, faithful to the legacy
/// `Batcher`:
///
///   - The [OverflowPolicy] axis governs a full buffer: `DROP_OLDEST` evicts the head, `DROP_NEWEST`
///     drops the arriving batch, `BLOCK` applies backpressure until space frees, and `FAIL` refuses
///     the arriving batch retryably with a retry-after hint.
///   - The degraded-window axis, keyed strictly off downstream delivery health, refuses ingress after
///     a failed or rejected downstream delivery until the window elapses, then admits one probe. A
///     bursty producer that merely saturates the queue against a healthy but slow downstream trips the
///     overflow axis, not this breaker; a downstream that fails every delivery trips this breaker even
///     when the queue is not full.
///
/// The `waitForResult` flag on the policy selects the acknowledgement model. When true, the enqueued
/// entry carries a [CompletableFuture] the [#offer] stage resolves to once the draining worker records
/// the real downstream result. When false, [#offer] resolves immediately at enqueue and a later drain
/// failure is logged and counted but not signalled upstream.
///
/// @param <T> the payload type this queue delivers
public final class DeliveryQueue<T extends TelemetryPayload> {

    private static final Logger LOG = LoggerFactory.getLogger(DeliveryQueue.class);

    /// The retry-after hint a `FAIL`-overflow refusal carries. A full queue against a healthy
    /// downstream is a transient capacity condition, so the hint is a short fixed backpressure window
    /// rather than the degraded window, which is a separate downstream-health concern.
    private static final Duration FULL_QUEUE_RETRY_AFTER = Duration.ofMillis(100);

    /// The healthy sentinel for the degraded marker. A real `System.nanoTime` origin can be negative,
    /// so a plain `0` sentinel would misfire; `Long.MIN_VALUE` is unambiguously "not degraded".
    private static final long HEALTHY = Long.MIN_VALUE;

    private final BlockingQueue<QueueEntry<T>> queue;
    private final int numConsumers;
    private final OverflowPolicy overflow;
    private final boolean waitForResult;
    private final long degradedWindowNanos;
    private final SignalConsumer<T> next;
    private final Provenance provenance;
    private final LongSupplier nanoTime;
    private final List<Thread> workers = new ArrayList<>();
    private final AtomicInteger inFlight = new AtomicInteger();

    private volatile boolean accepting = true;
    private volatile long degradedUntilNanos = HEALTHY;

    /// Builds a delivery queue over `next`, sized and shaped by `policy`, with the degraded window and
    /// a nanosecond time source for the degraded marker.
    ///
    /// @param policy         the queue capacity, consumer count, overflow policy, and wait-for-result flag
    /// @param degradedWindow how long ingress is refused after a failed downstream delivery
    /// @param next           the downstream consumer the workers drain into
    /// @param provenance     the producing component's provenance, stamped onto every synthesized result
    /// @param nanoTime       the monotonic nanosecond source, injectable so the degraded window is testable
    public DeliveryQueue(
            DeliveryQueuePolicy policy,
            Duration degradedWindow,
            SignalConsumer<T> next,
            Provenance provenance,
            LongSupplier nanoTime) {
        if (degradedWindow == null || degradedWindow.isNegative()) {
            throw new IllegalArgumentException("degradedWindow must be non-null and non-negative");
        }
        this.queue = new ArrayBlockingQueue<>(policy.queueSize());
        this.numConsumers = policy.numConsumers();
        this.overflow = policy.overflow();
        this.waitForResult = policy.waitForResult();
        this.degradedWindowNanos = degradedWindow.toNanos();
        this.next = next;
        this.provenance = provenance;
        this.nanoTime = nanoTime;
    }

    /// Starts the drain workers. Each worker is a virtual thread that takes entries and delivers them
    /// downstream until it reads the shutdown poison entry.
    public void start() {
        for (var i = 0; i < numConsumers; i++) {
            var worker = Thread.ofVirtual().name("delivery-queue-" + i).unstarted(this::drainLoop);
            workers.add(worker);
            worker.start();
        }
    }

    /// Offers one payload for delivery, applying the degraded and overflow axes.
    ///
    /// The returned stage resolves at enqueue for the fire-and-forget model and for every immediate
    /// refusal (degraded, `FAIL` overflow, or a drop), and resolves to the real downstream result once
    /// drained for the wait-for-result model.
    ///
    /// @param context the delivery context to carry downstream
    /// @param payload the payload to deliver
    /// @return a stage resolving to the delivery result
    public CompletionStage<DeliveryResult> offer(ConsumeContext context, T payload) {
        if (!accepting) {
            return completed(DeliveryResult.shuttingDown("delivery queue is shutting down", provenance));
        }
        var until = degradedUntilNanos;
        if (until != HEALTHY) {
            var remaining = until - nanoTime.getAsLong();
            if (remaining > 0) {
                return completed(DeliveryResult.throttled(
                        Duration.ofNanos(remaining), "delivery queue degraded", provenance));
            }
        }
        var entry = new QueueEntry<>(context, payload, waitForResult ? new CompletableFuture<>() : null);
        return switch (overflow) {
            case BLOCK -> block(entry);
            case DROP_OLDEST -> dropOldest(entry);
            case DROP_NEWEST -> dropNewest(entry);
            case FAIL -> fail(entry);
        };
    }

    private CompletionStage<DeliveryResult> block(QueueEntry<T> entry) {
        try {
            queue.put(entry);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return completed(DeliveryResult.shuttingDown("interrupted enqueueing to delivery queue", provenance));
        }
        return admitted(entry);
    }

    private CompletionStage<DeliveryResult> dropOldest(QueueEntry<T> entry) {
        while (!queue.offer(entry)) {
            var dropped = queue.poll();
            if (dropped != null) {
                completeDropped(dropped);
            }
        }
        return admitted(entry);
    }

    private CompletionStage<DeliveryResult> dropNewest(QueueEntry<T> entry) {
        if (queue.offer(entry)) {
            return admitted(entry);
        }
        // The arriving batch is dropped. Nothing was enqueued, so its future (if any) is resolved here.
        return completed(droppedResult());
    }

    private CompletionStage<DeliveryResult> fail(QueueEntry<T> entry) {
        if (queue.offer(entry)) {
            return admitted(entry);
        }
        return completed(DeliveryResult.throttled(FULL_QUEUE_RETRY_AFTER, "delivery queue full", provenance));
    }

    /// The stage a successfully enqueued entry resolves to: the downstream future for the
    /// wait-for-result model, or an immediate acceptance for the fire-and-forget model.
    private CompletionStage<DeliveryResult> admitted(QueueEntry<T> entry) {
        var future = entry.future();
        if (future != null) {
            return future;
        }
        return completed(DeliveryResult.accepted(provenance));
    }

    /// Resolves the future of a batch dropped by overflow, and returns the result the fire-and-forget
    /// model reports for its own drop. A dropped batch never reaches the downstream, so it does not
    /// touch the degraded marker.
    private void completeDropped(QueueEntry<T> dropped) {
        var future = dropped.future();
        if (future != null) {
            future.complete(droppedResult());
        }
    }

    private DeliveryResult droppedResult() {
        return DeliveryResult.admissionRefused("delivery queue overflow, batch dropped", null, provenance);
    }

    private void drainLoop() {
        while (true) {
            QueueEntry<T> entry;
            try {
                entry = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (entry.payload() == null) {
                return;
            }
            inFlight.incrementAndGet();
            try {
                deliver(entry);
            } finally {
                inFlight.decrementAndGet();
            }
        }
    }

    private void deliver(QueueEntry<T> entry) {
        DeliveryResult result;
        try {
            result = next.consume(entry.context(), entry.payload()).toCompletableFuture().join();
        } catch (Throwable t) {
            result = DeliveryResult.permanentFrom("delivery queue drain", t, provenance);
        }
        recordOutcome(result);
        var future = entry.future();
        if (future != null) {
            future.complete(result);
        } else if (result instanceof DeliveryResult.Rejected rejected) {
            LOG.warn("delivery queue drain rejected downstream (not signalled upstream): {}", rejected.message());
        }
    }

    /// Updates the degraded marker off the downstream outcome: a rejection trips the window, any other
    /// outcome clears it, so the breaker tracks genuine downstream health rather than queue fullness.
    private void recordOutcome(DeliveryResult result) {
        if (result instanceof DeliveryResult.Rejected) {
            if (degradedWindowNanos > 0) {
                degradedUntilNanos = nanoTime.getAsLong() + degradedWindowNanos;
            }
        } else {
            degradedUntilNanos = HEALTHY;
        }
    }

    /// Waits until the queue is empty and no delivery is in flight, or the deadline elapses.
    ///
    /// @param deadline the maximum time to wait
    /// @return true if fully drained within the deadline
    public boolean awaitDrained(Duration deadline) {
        var deadlineNanos = System.nanoTime() + deadline.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (queue.isEmpty() && inFlight.get() == 0) {
                return true;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return queue.isEmpty() && inFlight.get() == 0;
    }

    /// Stops accepting new offers, drains the entries already queued into the still-open downstream,
    /// and stops the workers within the deadline. A poison entry per worker unblocks the workers once
    /// the real entries ahead of it have drained.
    ///
    /// @param deadline the maximum time to wait for the workers to finish
    public void drainAndStop(Duration deadline) {
        accepting = false;
        for (var i = 0; i < numConsumers; i++) {
            try {
                queue.put(new QueueEntry<>(null, null, null));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        var deadlineNanos = System.nanoTime() + deadline.toNanos();
        for (var worker : workers) {
            var remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            try {
                worker.join(Duration.ofNanos(remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static CompletionStage<DeliveryResult> completed(DeliveryResult result) {
        return CompletableFuture.completedFuture(result);
    }

    /// One queued unit: the delivery context and payload, plus the completion future the
    /// wait-for-result model resolves. A poison entry carries a `null` payload and null future.
    ///
    /// @param context the delivery context, null only for a poison entry
    /// @param payload the payload, null only for a poison entry
    /// @param future  the wait-for-result completion future, null for the fire-and-forget model
    /// @param <T>     the payload type
    private record QueueEntry<T extends TelemetryPayload>(
            @Nullable ConsumeContext context,
            @Nullable T payload,
            @Nullable CompletableFuture<DeliveryResult> future) {
    }
}
