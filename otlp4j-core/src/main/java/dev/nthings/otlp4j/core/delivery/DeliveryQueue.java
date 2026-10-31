package dev.nthings.otlp4j.core.delivery;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.policy.RedeliveryPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/// A bounded delivery boundary. Admission is acknowledged as soon as an entry enters this queue;
/// downstream completion is observed by the flush barrier and summarized in a [FlushReport]. There is
/// deliberately no blocking admission or result-waiting mode: callers either get a retryable refusal
/// (`FAIL`) or choose an explicit drop policy. A queue never declines to attempt an entry it admitted:
/// one failed delivery does not stop the entries behind it.
///
/// ## Who owns a refusal
///
/// A refusal belongs to the caller it is returned to. `FAIL` returns the retryable refusal and records
/// nothing: the sender is told, may retry, and a successful retry must not later show up as a loss.
/// `DROP_NEWEST` and `DROP_OLDEST` instead acknowledge the arriving entry and discard data, so those
/// drops are losses the queue owns. Only losses after admission are recorded in the [FlushReport].
///
/// ## Losses
///
/// Data lost after admission is never silent. Each such loss is part of the [FlushReport] and is passed
/// to the [LossListener] the owner supplies, which is where the owner logs and counts it; the queue is
/// in the contracts module and has no logging or metrics of its own. The listener runs before the
/// entry is completed, so a caller whose flush has returned also sees the loss in its instruments.
///
/// ## Redelivery
///
/// Without a [RedeliveryPolicy] an entry gets one attempt. With one, a retryable rejection is retried
/// by the same worker, which stays occupied for the wait so concurrency never exceeds the consumer
/// count. A permanent rejection, a full acceptance and a partial success are terminal; a partial
/// success is never retried because the accepted items would be delivered twice. The wait is the larger
/// of the jittered backoff and the rejection's retry-after hint, and the entry is given up as soon as
/// that wait would pass the policy's elapsed budget. The wait is on a stop signal of its own, not on
/// the monitor that every completion notifies. [#drainAndStop] raises the signal: no new wait starts, a
/// current wait ends, the entry that was waiting gets one last attempt, and each entry still queued
/// gets exactly one.
///
/// ## Barrier
///
/// Entries are assigned a monotonically increasing sequence under the admission lock. A flush swaps
/// the outcome accumulator and waits for all entries at or below its sequence barrier, so submissions
/// accepted after the call are excluded even when workers complete out of order. The pending set is
/// updated before an entry becomes visible to a worker and removed only after its downstream result is
/// recorded and its admission ownership released; this avoids the queue-empty/in-flight handoff race.
/// An entry being redelivered stays pending, so a flush waits for it up to the flush deadline.
public final class DeliveryQueue<T> {

    private static final System.Logger LOG = System.getLogger(DeliveryQueue.class.getName());
    private static final Duration FULL_QUEUE_RETRY_AFTER = Duration.ofMillis(100);

    private final BlockingQueue<QueueEntry<T>> queue;
    private final int capacity;
    private final int numConsumers;
    private final OverflowPolicy overflow;
    private final @Nullable RedeliveryPolicy redelivery;
    private final SignalConsumer<T> next;
    private final Provenance provenance;
    private final LossListener lossListener;
    private final Object monitor = new Object();
    // Raised once, by drainAndStop or a fatal worker error, to end every redelivery wait. Kept apart
    // from `monitor`, which is notified on every entry completion and would wake a waiting entry early.
    private final CountDownLatch stopSignal = new CountDownLatch(1);
    private final TreeSet<Long> pending = new TreeSet<>();
    private final List<Thread> workers = new ArrayList<>();
    private final AtomicLong nextSequence = new AtomicLong();
    private final AtomicReference<Accumulator> active = new AtomicReference<>(new Accumulator());
    // A timed-out barrier can be observed by a later flush. Keep that state coalesced so repeated
    // timeouts do not create an unbounded list of empty accumulators while a downstream call hangs.
    private @Nullable Accumulator carry;

    private volatile boolean accepting = true;

    /// Builds a queue that attempts each entry once. A capacity of [Integer#MAX_VALUE] makes the queue
    /// effectively unbounded without allocating for it up front, for an owner that bounds the work it
    /// admits some other way.
    ///
    /// @param policy       the capacity, consumer count and overflow behaviour
    /// @param next         the downstream consumer every admitted entry is delivered to
    /// @param provenance   the component identity attached to results
    /// @param lossListener told of every loss after admission; it must not block
    public DeliveryQueue(
            DeliveryQueuePolicy policy, SignalConsumer<T> next, Provenance provenance, LossListener lossListener) {
        this(policy, null, next, provenance, lossListener);
    }

    /// Builds a queue that redelivers an entry its consumer fails retryably, for as long as
    /// `redelivery` allows.
    ///
    /// @param policy       the capacity, consumer count and overflow behaviour
    /// @param redelivery   the redelivery schedule and budget, or null to attempt each entry once
    /// @param next         the downstream consumer every admitted entry is delivered to
    /// @param provenance   the component identity attached to results
    /// @param lossListener told of every loss after admission: a failed or partially rejected
    ///                     delivery, an entry given up by redelivery, evicted or discarded by an
    ///                     overflow policy, or abandoned by a fatal worker error; it must not block
    public DeliveryQueue(
            DeliveryQueuePolicy policy,
            @Nullable RedeliveryPolicy redelivery,
            SignalConsumer<T> next,
            Provenance provenance,
            LossListener lossListener) {
        if (policy == null || next == null || provenance == null || lossListener == null) {
            throw new IllegalArgumentException("policy, next, provenance and lossListener are required");
        }
        this.capacity = policy.queueSize();
        this.queue = new LinkedBlockingQueue<>(capacity);
        this.numConsumers = policy.numConsumers();
        this.overflow = policy.overflow();
        this.redelivery = redelivery;
        this.next = next;
        this.provenance = provenance;
        this.lossListener = lossListener;
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
        return offerAll(context, List.of(payload), () -> {}).stages().getFirst();
    }

    /// Atomically admits a group of split units. `FAIL` refuses the whole group when there is
    /// insufficient room, and the refusal is the caller's: it is returned, not recorded. `DROP_NEWEST`
    /// acknowledges the group and discards it, `DROP_OLDEST` evicts enough old units first and
    /// acknowledges the group; both record what they drop. A queue that has stopped accepting returns a
    /// retryable shutdown refusal. The shared callback is run exactly once after all admitted units
    /// complete or are dropped.
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
            return offerAllLocked(context, payloads, onComplete);
        }
    }

    private OfferBatch<T> offerAllLocked(
            ConsumeContext context, List<Payload<T>> payloads, Runnable onComplete) {
        var count = payloads.size();
        if (count > capacity) {
            return overflow == OverflowPolicy.DROP_NEWEST
                    ? discard(payloads, onComplete, "delivery queue overflow: the group exceeds the queue capacity")
                    : refused(DeliveryResult.throttled(
                            FULL_QUEUE_RETRY_AFTER, "delivery queue group exceeds capacity", provenance));
        }
        if (overflow != OverflowPolicy.DROP_OLDEST && queue.remainingCapacity() < count) {
            return overflow == OverflowPolicy.DROP_NEWEST
                    ? discard(payloads, onComplete, "delivery queue overflow: the newest entry was dropped")
                    : refused(DeliveryResult.throttled(FULL_QUEUE_RETRY_AFTER, "delivery queue full", provenance));
        }
        if (overflow == OverflowPolicy.DROP_OLDEST) {
            while (queue.remainingCapacity() < count) {
                var dropped = queue.poll();
                if (dropped == null) {
                    continue;
                }
                dropped.accumulator().drop(dropped.payload().itemCount());
                lost(dropped.payload().itemCount(), "evicted from a full delivery queue by DROP_OLDEST");
                completeEntry(dropped);
            }
        }
        var tracker = new GroupCompletion(count, onComplete);
        var stages = new ArrayList<CompletionStage<DeliveryResult>>(count);
        var admitted = new ArrayList<QueueEntry<T>>(count);
        for (var payload : payloads) {
            var sequence = nextSequence.getAndIncrement();
            var entry = new QueueEntry<>(sequence, context, payload, active.get(), tracker::complete);
            pending.add(sequence);
            if (!queue.offer(entry)) {
                // The only cause is a concurrent queue implementation failure; preserve the atomic
                // group guarantee by taking back the entries inserted during this attempt. The group is
                // refused, so its completion callback is the caller's to run, as for any refusal.
                pending.remove(sequence);
                for (var admittedEntry : admitted) {
                    if (queue.remove(admittedEntry)) {
                        pending.remove(admittedEntry.sequence());
                    }
                }
                return refused(DeliveryResult.throttled(
                        FULL_QUEUE_RETRY_AFTER, "delivery queue full", provenance));
            }
            admitted.add(entry);
            stages.add(completed(DeliveryResult.accepted(provenance)));
        }
        return new OfferBatch<>(List.copyOf(stages), DeliveryResult.accepted(provenance));
    }

    /// A refusal returned to the caller. It is deliberately not recorded: the caller was told and owns
    /// the retry, so recording it would report a loss for data the sender later delivers.
    private OfferBatch<T> refused(DeliveryResult result) {
        return new OfferBatch<>(List.of(completed(result)), result);
    }

    /// `DROP_NEWEST`: acknowledges the group and discards it. Unlike a refusal, nobody is told to retry,
    /// so this is a loss the queue owns.
    private OfferBatch<T> discard(List<Payload<T>> payloads, Runnable onComplete, String reason) {
        var items = 0L;
        for (var payload : payloads) {
            items += payload.itemCount();
        }
        active.get().drop(items);
        lost(items, reason);
        onComplete.run();
        var stages = new ArrayList<CompletionStage<DeliveryResult>>(payloads.size());
        for (var ignored : payloads) {
            stages.add(completed(DeliveryResult.accepted(provenance)));
        }
        return new OfferBatch<>(List.copyOf(stages), DeliveryResult.accepted(provenance));
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

    /// Delivers one entry: a single attempt, or with redelivery configured, attempts until a terminal
    /// outcome, the elapsed budget, or shutdown ends it. The worker stays on the entry throughout.
    private void deliver(QueueEntry<T> entry) {
        var startedAt = System.nanoTime();
        var attempts = 0;
        var lastChance = false;
        @Nullable String gaveUp = null;
        DeliveryResult result;
        while (true) {
            attempts++;
            result = attempt(entry);
            if (redelivery == null || !retryable(result)) {
                break;
            }
            if (lastChance || stopSignal.getCount() == 0) {
                gaveUp = "redelivery stopped by shutdown after " + attempts + " attempts";
                break;
            }
            var delay = redeliveryDelay(redelivery, attempts, ((DeliveryResult.Rejected) result).retryAfter());
            var remainingBudget = redelivery.maxElapsed().minusNanos(System.nanoTime() - startedAt);
            if (delay.compareTo(remainingBudget) > 0) {
                gaveUp = "redelivery budget of " + redelivery.maxElapsed() + " exhausted after " + attempts
                        + " attempts, the next wait of " + delay + " would pass it";
                break;
            }
            var wait = awaitStop(delay);
            if (wait == Wait.INTERRUPTED) {
                gaveUp = "redelivery interrupted after " + attempts + " attempts";
                break;
            }
            lastChance = wait == Wait.STOPPED;
        }
        var items = entry.payload().itemCount();
        synchronized (monitor) {
            // Keep outcome recording in the admission monitor. A timed-out flush may redirect this
            // accumulator into the carry accumulator; serializing the two operations prevents a late
            // delivery from updating an accumulator that was already merged and forgotten.
            entry.accumulator().record(items, result);
        }
        // Report the loss before completing the entry, so a caller whose flush has returned also sees
        // it in the owner's instruments.
        if (result instanceof DeliveryResult.Rejected rejected) {
            lost(items, (gaveUp != null ? gaveUp + ": " : "downstream rejected the delivery: ") + rejected.message());
        } else if (result instanceof DeliveryResult.Partial partial && partial.rejectedItems() > 0) {
            lost(partial.rejectedItems(), "downstream rejected part of the delivery: " + partial.message());
        }
        completeEntry(entry);
    }

    private DeliveryResult attempt(QueueEntry<T> entry) {
        try {
            var result = next.consume(entry.context(), entry.payload()).toCompletableFuture().join();
            return result != null
                    ? result
                    : DeliveryResult.permanent("downstream returned a null delivery result", provenance);
        } catch (Throwable failure) {
            try {
                return DeliveryResult.fromFailure("delivery queue drain", failure, provenance);
            } catch (Error fatal) {
                // fromFailure deliberately rethrows fatal VM errors. The queue still has to
                // release this entry before the worker dies, otherwise a flush barrier would wait
                // forever and the global admission accounting would remain occupied.
                completeFatal(entry, fatal);
                throw fatal;
            }
        }
    }

    private static boolean retryable(DeliveryResult result) {
        return result instanceof DeliveryResult.Rejected rejected && rejected.disposition() == Disposition.RETRYABLE;
    }

    /// The wait before the next attempt: the larger of the jittered exponential backoff and the
    /// failure's own retry-after hint, as the upstream Collector's retry sender does.
    private static Duration redeliveryDelay(RedeliveryPolicy policy, int attempt, @Nullable Duration retryAfter) {
        var backoff = policy.backoff();
        var capNanos = (double) backoff.max().toNanos();
        var base = Math.min(capNanos, backoff.initial().toNanos() * Math.pow(backoff.multiplier(), attempt - 1));
        var jitter = backoff.jitter();
        var spread = 1.0 - jitter + 2.0 * jitter * ThreadLocalRandom.current().nextDouble();
        // The documented contract of a backoff is that no wait exceeds its cap, jitter included.
        var jittered = Duration.ofNanos(Math.max(1L, (long) Math.min(capNanos, base * spread)));
        return retryAfter != null && retryAfter.compareTo(jittered) > 0 ? retryAfter : jittered;
    }

    private enum Wait { ELAPSED, STOPPED, INTERRUPTED }

    private Wait awaitStop(Duration delay) {
        try {
            return stopSignal.await(delay.toNanos(), TimeUnit.NANOSECONDS) ? Wait.STOPPED : Wait.ELAPSED;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Wait.INTERRUPTED;
        } catch (ArithmeticException tooLong) {
            // A delay beyond what a long of nanoseconds holds is beyond any elapsed budget too, which
            // the caller has already checked; waiting for the stop signal alone is the faithful reading.
            return awaitStopForever();
        }
    }

    private Wait awaitStopForever() {
        try {
            stopSignal.await();
            return Wait.STOPPED;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Wait.INTERRUPTED;
        }
    }

    private void lost(long items, String reason) {
        try {
            lossListener.lost(items, reason);
        } catch (RuntimeException listenerFailure) {
            // A broken instrument must not stop the worker that is draining accepted work.
            LOG.log(System.Logger.Level.WARNING,
                    "delivery queue " + provenance.componentId() + " loss listener failed", listenerFailure);
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
            abandoned.add(entry);
            QueueEntry<T> queued;
            while ((queued = queue.poll()) != null) {
                abandoned.add(queued);
                queued.accumulator().reject(queued.payload().itemCount(), fatal);
            }
        }
        // Wake any entry waiting out a backoff: with a worker dead and admission closed, it gets its
        // one last attempt now rather than holding the queue open for the rest of its wait.
        stopSignal.countDown();
        var abandonedItems = 0L;
        for (var queued : abandoned) {
            abandonedItems += queued.payload().itemCount();
        }
        lost(abandonedItems, "delivery worker died of a fatal error: " + fatal);
        completeAbandoned(abandoned, fatal);
    }

    private void completeAbandoned(List<QueueEntry<T>> abandoned, Throwable cause) {
        for (var queued : abandoned) {
            try {
                completeEntry(queued);
            } catch (Throwable cleanupFailure) {
                if (cleanupFailure != cause) {
                    cause.addSuppressed(cleanupFailure);
                }
            }
        }
    }

    /// Takes every entry still waiting for a consumer once a drain has run out of time and the workers
    /// are interrupted: no worker will take them, so they are lost, and said to be, rather than left
    /// pending forever.
    private void abandonQueued(Throwable cause) {
        var abandoned = new ArrayList<QueueEntry<T>>();
        var failure = new IllegalStateException(
                "delivery queue was not drained before its shutdown deadline: " + cause.getMessage());
        var items = 0L;
        synchronized (monitor) {
            QueueEntry<T> queued;
            while ((queued = queue.poll()) != null) {
                abandoned.add(queued);
                items += queued.payload().itemCount();
                queued.accumulator().reject(queued.payload().itemCount(), failure);
            }
        }
        if (abandoned.isEmpty()) {
            return;
        }
        lost(items, "abandoned at shutdown: the delivery queue did not drain before its deadline");
        completeAbandoned(abandoned, failure);
    }

    private void completeEntry(QueueEntry<T> entry) {
        try {
            entry.onComplete().run();
        } finally {
            synchronized (monitor) {
                pending.remove(entry.sequence());
                monitor.notifyAll();
            }
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
    /// Redelivery stops with it: each entry gets at most one attempt more, and an entry whose backoff
    /// was running gets that one immediately. A timeout is exceptional; the workers are interrupted and
    /// the entries still waiting for one are lost and reported as such. Completed outcomes remain
    /// observable through a later report only when the barrier drained.
    public CompletionStage<FlushReport> drainAndStop(Duration deadline) {
        synchronized (monitor) {
            accepting = false;
            monitor.notifyAll();
        }
        // No redelivery wait starts after this, and one in progress ends with its entry's last attempt.
        stopSignal.countDown();
        // Return the stage the cleanup completes, not the flush itself, so a caller told of the timeout
        // already finds the abandoned entries counted.
        return flush(deadline).whenComplete((ignored, failure) -> {
            if (failure != null) {
                for (var worker : workers) {
                    worker.interrupt();
                }
                abandonQueued(failure);
            }
        });
    }

    /// The entries admitted and not yet finished: waiting for a consumer, being delivered, or waiting
    /// out a redelivery backoff. This is the depth to watch for an outage; it can exceed the queue
    /// capacity by up to the consumer count, because an entry a consumer has taken occupies no slot.
    ///
    /// @return the number of unfinished entries
    public int size() {
        synchronized (monitor) {
            return pending.size();
        }
    }

    private static long saturatingDeadline(Duration duration) {
        var now = System.nanoTime();
        try {
            return Math.addExact(now, duration.toNanos());
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
