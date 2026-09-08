package dev.nthings.otlp4j.components.internal;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The bounded delivery boundary used by the batch processor. Admission is acknowledged as soon as
/// an entry enters this queue; downstream completion is observed by the flush barrier and summarized
/// in a [FlushReport]. There is deliberately no blocking admission or result-waiting mode: callers
/// either get a retryable refusal (`FAIL`) or choose an explicit drop policy.
///
/// Entries are assigned a monotonically increasing sequence under the admission lock. A flush swaps
/// the outcome accumulator and waits for all entries at or below its sequence barrier, so submissions
/// accepted after the call are excluded even when workers complete out of order. The pending set is
/// updated before an entry becomes visible to a worker and removed only after its downstream result is
/// recorded; this avoids the queue-empty/in-flight handoff race.
public final class DeliveryQueue<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DeliveryQueue.class);
    private static final Duration FULL_QUEUE_RETRY_AFTER = Duration.ofMillis(100);
    private static final long HEALTHY = Long.MIN_VALUE;

    private final BlockingQueue<QueueEntry<T>> queue;
    private final int capacity;
    private final int numConsumers;
    private final OverflowPolicy overflow;
    private final long degradedWindowNanos;
    private final SignalConsumer<T> next;
    private final Provenance provenance;
    private final LongSupplier nanoTime;
    private final Object monitor = new Object();
    private final TreeSet<Long> pending = new TreeSet<>();
    private final List<Thread> workers = new ArrayList<>();
    private final AtomicLong nextSequence = new AtomicLong();
    private final AtomicReference<Accumulator> active = new AtomicReference<>(new Accumulator());
    // A timed-out barrier can be observed by a later flush. Keep that state coalesced so repeated
    // timeouts do not create an unbounded list of empty accumulators while a downstream call hangs.
    private @Nullable Accumulator carry;

    private volatile boolean accepting = true;
    private volatile long degradedUntilNanos = HEALTHY;

    /// Builds a queue over `next`, sized and shaped by `policy`.
    public DeliveryQueue(
            DeliveryQueuePolicy policy,
            Duration degradedWindow,
            SignalConsumer<T> next,
            Provenance provenance,
            LongSupplier nanoTime) {
        if (policy == null || degradedWindow == null || next == null || provenance == null || nanoTime == null) {
            throw new IllegalArgumentException("policy, degradedWindow, next, provenance and nanoTime are required");
        }
        if (degradedWindow.isNegative()) {
            throw new IllegalArgumentException("degradedWindow must be non-negative");
        }
        this.capacity = policy.queueSize();
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.numConsumers = policy.numConsumers();
        this.overflow = policy.overflow();
        this.degradedWindowNanos = degradedWindow.toNanos();
        this.next = next;
        this.provenance = provenance;
        this.nanoTime = nanoTime;
    }

    /// Starts the virtual delivery workers. Calling this more than once is an error.
    public synchronized void start() {
        if (!workers.isEmpty()) {
            throw new IllegalStateException("delivery queue already started");
        }
        for (var i = 0; i < numConsumers; i++) {
            var worker = Thread.ofVirtual().name("delivery-queue-" + i).unstarted(this::drainLoop);
            workers.add(worker);
            worker.start();
        }
    }

    /// Attempts bounded admission. A successful result means only that the entry has entered the
    /// queue; downstream failures are reported by [#flush(Duration)].
    public CompletionStage<DeliveryResult> offer(ConsumeContext context, Payload<T> payload) {
        if (context == null || payload == null) {
            throw new IllegalArgumentException("context and payload must be non-null");
        }
        synchronized (monitor) {
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

            return offerAllLocked(context, List.of(payload), () -> {}).stages().get(0);
        }
    }

    /// Atomically admits a group of split units. `FAIL` and `DROP_NEWEST` refuse the whole group when
    /// there is insufficient room; `DROP_OLDEST` evicts enough old units first. The shared callback is
    /// run exactly once after all admitted units complete or are evicted.
    public OfferBatch<T> offerAll(
            ConsumeContext context, List<Payload<T>> payloads, Runnable onComplete) {
        if (context == null || payloads == null || payloads.isEmpty() || onComplete == null) {
            throw new IllegalArgumentException("context, payloads and onComplete are required");
        }
        for (var payload : payloads) {
            if (payload == null) {
                throw new IllegalArgumentException("payloads must not contain null");
            }
        }
        synchronized (monitor) {
            if (!accepting) {
                return refused(DeliveryResult.shuttingDown("delivery queue is shutting down", provenance));
            }
            var until = degradedUntilNanos;
            if (until != HEALTHY) {
                var remaining = until - nanoTime.getAsLong();
                if (remaining > 0) {
                    return refused(
                            DeliveryResult.throttled(
                                    Duration.ofNanos(remaining), "delivery queue degraded", provenance),
                            payloads);
                }
            }
            return offerAllLocked(context, payloads, onComplete);
        }
    }

    private OfferBatch<T> offerAllLocked(
            ConsumeContext context, List<Payload<T>> payloads, Runnable onComplete) {
        var count = payloads.size();
        if (count > capacity) {
            var refusal = overflow == OverflowPolicy.DROP_NEWEST
                    ? droppedResult()
                    : DeliveryResult.throttled(FULL_QUEUE_RETRY_AFTER, "delivery queue group exceeds capacity", provenance);
            return refused(refusal, payloads);
        }
        if (overflow != OverflowPolicy.DROP_OLDEST && queue.remainingCapacity() < count) {
            var refusal = overflow == OverflowPolicy.DROP_NEWEST
                    ? droppedResult()
                    : DeliveryResult.throttled(FULL_QUEUE_RETRY_AFTER, "delivery queue full", provenance);
            return refused(refusal, payloads);
        }
        if (overflow == OverflowPolicy.DROP_OLDEST) {
            while (queue.remainingCapacity() < count) {
                var dropped = queue.poll();
                if (dropped == null) {
                    continue;
                }
                dropLocked(dropped);
            }
        }
        var tracker = new GroupCompletion(count, onComplete);
        var stages = new ArrayList<CompletionStage<DeliveryResult>>(count);
        var admitted = new ArrayList<QueueEntry<T>>(count);
        for (var payload : payloads) {
            if (payload == null) {
                throw new IllegalArgumentException("payloads must not contain null");
            }
            var sequence = nextSequence.getAndIncrement();
            var entry = new QueueEntry<>(sequence, context, payload, active.get(), tracker::complete);
            pending.add(sequence);
            if (!queue.offer(entry)) {
                // The only cause is a concurrent queue implementation failure; preserve the atomic
                // group guarantee by removing entries inserted during this attempt.
                pending.remove(sequence);
                for (var admittedEntry : admitted) {
                    if (queue.remove(admittedEntry)) {
                        dropLocked(admittedEntry);
                    }
                }
                tracker.complete();
                return refused(DeliveryResult.throttled(
                        FULL_QUEUE_RETRY_AFTER, "delivery queue full", provenance));
            }
            admitted.add(entry);
            stages.add(completed(DeliveryResult.accepted(provenance)));
        }
        return new OfferBatch<>(List.copyOf(stages), DeliveryResult.accepted(provenance));
    }

    private OfferBatch<T> refused(DeliveryResult result) {
        return new OfferBatch<>(List.of(completed(result)), result);
    }

    private OfferBatch<T> refused(DeliveryResult result, List<Payload<T>> payloads) {
        var items = payloads.stream().mapToLong(Payload::itemCount).sum();
        if (result instanceof DeliveryResult.Rejected rejected
                && rejected.reason() == DeliveryReason.ADMISSION_REFUSED) {
            active.get().drop(items);
        } else if (result instanceof DeliveryResult.Rejected rejected) {
            active.get().reject(items, rejected.cause() != null
                    ? rejected.cause()
                    : new IllegalStateException(rejected.message()));
        }
        return refused(result);
    }

    private void dropLocked(QueueEntry<T> dropped) {
        pending.remove(dropped.sequence());
        dropped.accumulator().drop(dropped.payload().itemCount());
        dropped.onComplete().run();
        monitor.notifyAll();
    }

    private DeliveryResult droppedResult() {
        return DeliveryResult.admissionRefused("delivery queue overflow, batch dropped", null, provenance);
    }

    private void drainLoop() {
        while (true) {
            if (!accepting && isDrained()) {
                return;
            }
            QueueEntry<T> entry;
            try {
                entry = queue.poll(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (entry == null) {
                continue;
            }
            deliver(entry);
        }
    }

    private boolean isDrained() {
        synchronized (monitor) {
            return pending.isEmpty();
        }
    }

    private void deliver(QueueEntry<T> entry) {
        DeliveryResult result;
        try {
            result = next.consume(entry.context(), entry.payload()).toCompletableFuture().join();
            if (result == null) {
                result = DeliveryResult.permanent("downstream returned a null delivery result", provenance);
            }
        } catch (Throwable failure) {
            try {
                result = DeliveryResult.permanentFrom("delivery queue drain", failure, provenance);
            } catch (Error fatal) {
                // permanentFrom deliberately rethrows fatal VM errors. The queue still has to
                // release this entry before the worker dies, otherwise a flush barrier would wait
                // forever and the global admission accounting would remain occupied.
                completeFatal(entry, fatal);
                throw fatal;
            }
        }
        synchronized (monitor) {
            // Keep outcome recording in the admission monitor. A timed-out flush may redirect this
            // accumulator into the carry accumulator; serializing the two operations prevents a late
            // delivery from updating an accumulator that was already merged and forgotten.
            entry.accumulator().record(entry.payload().itemCount(), result);
            pending.remove(entry.sequence());
            if (result instanceof DeliveryResult.Rejected) {
                tripDegradedLocked();
            } else {
                clearDegradedIfExpiredLocked();
            }
            monitor.notifyAll();
        }
        entry.onComplete().run();
        if (result instanceof DeliveryResult.Rejected rejected) {
            LOG.warn("delivery queue drain rejected downstream for {}: {}", provenance.componentId(), rejected.message());
        }
    }

    private void completeFatal(QueueEntry<T> entry, Error fatal) {
        var abandoned = new ArrayList<QueueEntry<T>>();
        synchronized (monitor) {
            // A fatal VM error terminates this worker. Stop admission immediately so callers cannot
            // enqueue work into a queue that may now have no live consumer, and classify every entry
            // still sitting in the bounded queue against the same failure accumulator.
            accepting = false;
            entry.accumulator().reject(entry.payload().itemCount(), fatal);
            pending.remove(entry.sequence());
            QueueEntry<T> queued;
            while ((queued = queue.poll()) != null) {
                abandoned.add(queued);
                queued.accumulator().reject(queued.payload().itemCount(), fatal);
                pending.remove(queued.sequence());
            }
            monitor.notifyAll();
        }
        entry.onComplete().run();
        for (var queued : abandoned) {
            queued.onComplete().run();
        }
    }

    private void tripDegradedLocked() {
        if (degradedWindowNanos > 0) {
            var candidate = saturatingAdd(nanoTime.getAsLong(), degradedWindowNanos);
            degradedUntilNanos = Math.max(degradedUntilNanos, candidate);
        }
    }

    private void clearDegradedIfExpiredLocked() {
        if (degradedUntilNanos != HEALTHY && nanoTime.getAsLong() >= degradedUntilNanos) {
            degradedUntilNanos = HEALTHY;
        }
    }

    /// Waits for entries admitted before this call and returns their bounded outcome report. The stage
    /// fails with a timeout when the barrier cannot drain before the deadline.
    public CompletionStage<FlushReport> flush(Duration deadline) {
        if (deadline == null || deadline.isNegative()) {
            throw new IllegalArgumentException("deadline must be non-negative");
        }
        final long target;
        final long deadlineNanos = saturatingDeadline(deadline);
        final Accumulator accumulator;
        final @Nullable Accumulator previous;
        synchronized (monitor) {
            target = nextSequence.get() - 1;
            accumulator = active.getAndSet(new Accumulator());
            previous = carry;
            carry = null;
        }
        var future = new CompletableFuture<FlushReport>();
        Thread.startVirtualThread(() -> awaitBarrier(target, accumulator, previous, deadlineNanos, future));
        return future;
    }

    private void awaitBarrier(
            long target,
            Accumulator accumulator,
            @Nullable Accumulator previous,
            long deadlineNanos,
            CompletableFuture<FlushReport> result) {
        boolean drained = false;
        synchronized (monitor) {
            while (!pending.headSet(target + 1).isEmpty()) {
                var remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    var millis = Math.max(1, Math.min(100, Duration.ofNanos(remaining).toMillis()));
                    monitor.wait(millis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            drained = pending.headSet(target + 1).isEmpty();
            if (!drained) {
                // Preserve the live accumulator objects owned by pending entries. Redirecting under
                // the same monitor used by deliver() makes late outcomes land in the carried report;
                // copying counters here would orphan those outcomes after this timeout.
                if (previous != null) {
                    accumulator.redirectTo(previous);
                }
                if (carry == null) {
                    carry = previous == null ? accumulator : previous;
                } else if (carry != (previous == null ? accumulator : previous)) {
                    (previous == null ? accumulator : previous).redirectTo(carry);
                }
            }
        }
        if (!drained) {
            result.completeExceptionally(new TimeoutException("delivery queue flush deadline exceeded"));
            return;
        }
        var report = accumulator.report();
        if (previous != null) {
            report = previous.report().plus(report);
        }
        result.complete(report);
    }

    /// Stops admission, drains the outstanding entries within the deadline, and terminates workers.
    /// A timeout is exceptional; any completed outcomes remain observable through a later report only
    /// when the barrier drained.
    public CompletionStage<FlushReport> drainAndStop(Duration deadline) {
        synchronized (monitor) {
            accepting = false;
            monitor.notifyAll();
        }
        var future = flush(deadline);
        future.whenComplete((ignored, failure) -> {
            if (failure != null) {
                for (var worker : workers) {
                    worker.interrupt();
                }
            }
        });
        return future;
    }

    private static long saturatingDeadline(Duration duration) {
        var now = System.nanoTime();
        try {
            return Math.addExact(now, duration.toNanos());
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long saturatingAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static <T> CompletionStage<DeliveryResult> completed(DeliveryResult result) {
        return CompletableFuture.completedFuture(result);
    }

    /// The result of a grouped admission attempt.
    public record OfferBatch<T>(List<CompletionStage<DeliveryResult>> stages, DeliveryResult result) {
        public OfferBatch {
            stages = List.copyOf(stages);
        }
    }

    private record QueueEntry<T>(
            long sequence,
            ConsumeContext context,
            Payload<T> payload,
            Accumulator accumulator,
            Runnable onComplete) {}

    private static final class GroupCompletion {
        private final AtomicLong remaining;
        private final Runnable callback;

        private GroupCompletion(long count, Runnable callback) {
            this.remaining = new AtomicLong(count);
            this.callback = callback;
        }

        void complete() {
            if (remaining.decrementAndGet() == 0) {
                callback.run();
            }
        }
    }

    private static final class Accumulator {
        private final AtomicLong rejectedItems = new AtomicLong();
        private final AtomicLong droppedItems = new AtomicLong();
        private final AtomicLong failedBatches = new AtomicLong();
        private final AtomicReference<@Nullable Throwable> firstFailure = new AtomicReference<>();
        private volatile @Nullable Accumulator delegate;

        void drop(long items) {
            var root = root();
            if (root != this) {
                root.drop(items);
                return;
            }
            droppedItems.addAndGet(items);
            failedBatches.incrementAndGet();
            firstFailure.compareAndSet(null, new IllegalStateException("delivery queue overflow dropped a batch"));
        }

        void record(long items, DeliveryResult result) {
            var root = root();
            if (root != this) {
                root.record(items, result);
                return;
            }
            if (result instanceof DeliveryResult.Partial partial) {
                rejectedItems.addAndGet(partial.rejectedItems());
            } else if (result instanceof DeliveryResult.Rejected rejected) {
                rejectedItems.addAndGet(items);
                failedBatches.incrementAndGet();
                var cause = rejected.cause();
                firstFailure.compareAndSet(
                        null, cause != null ? cause : new IllegalStateException(rejected.message()));
            }
        }

        void reject(long items, Throwable failure) {
            var root = root();
            if (root != this) {
                root.reject(items, failure);
                return;
            }
            rejectedItems.addAndGet(items);
            failedBatches.incrementAndGet();
            firstFailure.compareAndSet(null, failure);
        }

        void merge(Accumulator other) {
            var target = root();
            var source = other.root();
            if (target != this) {
                target.merge(source);
                return;
            }
            if (source == this) {
                return;
            }
            rejectedItems.addAndGet(source.rejectedItems.get());
            droppedItems.addAndGet(source.droppedItems.get());
            failedBatches.addAndGet(source.failedBatches.get());
            var failure = source.firstFailure.get();
            if (failure != null) {
                firstFailure.compareAndSet(null, failure);
            }
        }

        FlushReport report() {
            var root = root();
            return root == this
                    ? new FlushReport(
                            rejectedItems.get(), droppedItems.get(), failedBatches.get(), firstFailure.get())
                    : root.report();
        }

        void redirectTo(Accumulator target) {
            var source = root();
            var destination = target.root();
            if (source == destination) {
                return;
            }
            destination.merge(source);
            source.delegate = destination;
        }

        private Accumulator root() {
            var next = delegate;
            if (next == null) {
                return this;
            }
            var root = next.root();
            delegate = root;
            return root;
        }
    }

}
