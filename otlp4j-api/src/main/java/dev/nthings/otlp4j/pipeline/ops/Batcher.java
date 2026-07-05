package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.Processor;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Signal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// An asynchronous, queue-backed, timer-triggered batching processor.
///
/// The [Builder#ackMode] selects when `consume`'s ack completes:
///
///   - [AckMode#ON_DELIVERY] (the default) holds the ack open until the batch is drained and
///     delivered downstream, and completes it with the real downstream result, so a sender learns
///     of a downstream failure and retains its data. The first-drain latency is bounded by
///     [Builder#flushInterval] and, once a downstream stage is in flight, by [Builder#deliveryTimeout]
///     (when set). Enable [Builder#reflectDownstreamFailures] alongside it for proactive ingress
///     rejection after a sustained outage.
///   - [AckMode#ON_ENQUEUE] completes the ack `Accepted` the moment the batch is queued, before
///     delivery. A drain that later fails downstream loses the batch with no upstream signal
///     (surfaced only through [#drainFailedCount] and the WARN log); only [#forceFlush] and
///     [#shutdown] surface a [BatchDeliveryException]. Lossy and fast: a tap-like posture.
///
/// > **Anti-pattern under [AckMode#ON_DELIVERY]:** a single-threaded producer that joins on each
/// > batch's ack before sending the next **deadlocks** when `flushThreshold > 1` and no timer or
/// > `forceFlush` intervenes, because batch *i*'s ack cannot resolve until batches *i+1..N* are
/// > enqueued. A real gateway dispatches concurrently (one virtual thread per inbound request), so
/// > producers *i* and *i+1* are different threads and the deadlock cannot form. A custom downstream
/// > that never completes its stage strands every merged caller's ack; bound it with
/// > [Builder#deliveryTimeout] (the bundled transports complete on a per-call virtual thread and do
/// > not need it).
///
/// Threading: an owned or the [BatchScheduler#shared] fallback scheduler drives the flush timer.
/// Pass [Builder#scheduler] a [BatchScheduler] to isolate a gateway's timer thread (the batcher
/// uses but does not own it: close it via [dev.nthings.otlp4j.pipeline.Wiring.Builder#owns] or by
/// hand, after shutting the batcher down); without one, the process-wide `otlp4j-batch-timer`
/// fallback is used. Each instance adds only a single cheap virtual thread for its serial drain.
public final class Batcher<T> implements Processor<T>, Sink<T>, Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Batcher.class);

    /// The default drain-timer interval, used when [Builder#flushInterval] is not set. Also the
    /// default retry-after hint on a [OverflowPolicy#FAIL] overflow rejection: a full queue is
    /// drained within one timer tick, so retrying after one interval is the earliest attempt that
    /// can succeed.
    static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofSeconds(1);

    private final Signal<T> signal;
    /// The drain target, late-bound: set at build when `.downstream(...)` was used (bound Sink form),
    /// or captured on the first [#process] / [#asSink] call (mid-chain form). An [AtomicReference] so
    /// the bind is race-free - a concurrent first-`process` from two chains cannot both win.
    private final AtomicReference<@Nullable Sink<? super T>> next = new AtomicReference<>();
    private final int flushThreshold;
    private final ArrayBlockingQueue<QueueEntry<T>> queue;
    private final OverflowPolicy overflowPolicy;
    private final boolean reflectDownstreamFailures;
    private final AckMode ackMode;
    private final Duration flushInterval;
    private final @Nullable Duration deliveryTimeout;
    private final LongAdder drops = new LongAdder();
    private final LongAdder drainFailed = new LongAdder();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService drainExecutor;
    private final BatchDrainEngine<T> drainEngine;

    /// Degraded window: a failed drain sets it one flush interval into the future; a successful
    /// drain clears it. While the window is open and [Builder#reflectDownstreamFailures] is on,
    /// `consume` rejects retryably instead of enqueueing. The window expiring by itself admits a
    /// probe batch, so a healed downstream recovers without a stuck state.
    private volatile long degradedUntilNanos;
    private volatile @Nullable Throwable lastDrainFailure;

    private Batcher(Signal<T> signal, Builder<T> b) {
        this.signal = signal;
        if (b.downstream != null) {
            next.set(b.downstream);   // bound (terminal) form: bind at build
        }
        this.flushThreshold = b.flushThreshold;
        this.queue = new ArrayBlockingQueue<>(b.queueCapacity);
        this.overflowPolicy = b.overflowPolicy;
        this.reflectDownstreamFailures = b.reflectDownstreamFailures;
        this.ackMode = b.ackMode;
        this.flushInterval = b.flushInterval;
        this.deliveryTimeout = b.deliveryTimeout;
        // The batcher uses but never owns the scheduler; without an explicit one it falls back to
        // the process-wide shared singleton (identical to the pre-F4 SharedTimer behavior).
        var scheduler = b.scheduler != null ? b.scheduler : BatchScheduler.shared();
        this.drainExecutor = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("otlp4j-batcher-drain").factory());
        this.drainEngine = new BatchDrainEngine<>(
                queue, next::get, signal::merge, closed, scheduler.executor(), drainExecutor,
                this::flushIfDue, this::onDrainOutcome, flushInterval, deliveryTimeout);
    }

    /// Tracks downstream health from drain outcomes and counts batches lost to a failed drain.
    private void onDrainOutcome(@Nullable Throwable failure, int batchCount) {
        if (failure == null) {
            degradedUntilNanos = 0L;
            lastDrainFailure = null;
        } else {
            drainFailed.add(batchCount);
            lastDrainFailure = failure;
            degradedUntilNanos = System.nanoTime() + flushInterval.toNanos();
        }
    }

    /// Processor entry (mid-chain): binds the continuation as `next` on first use, then enqueues
    /// exactly as the Sink form. Within one pipeline chain `next` is a single stable continuation.
    @Override
    public CompletionStage<ConsumeResult> process(T batch, Sink<? super T> forward) {
        bindNext(forward);
        return enqueue(batch);
    }

    /// Sink entry (bound form): requires a downstream bound via the builder; delegates to the same path.
    @Override
    public CompletionStage<ConsumeResult> consume(T batch) {
        if (next.get() == null) {
            throw new IllegalStateException(
                    "Batcher used as a Sink without a bound downstream; set .downstream(...) at build, "
                    + "or attach the mid-chain form via Stage.via(...)");
        }
        return enqueue(batch);
    }

    /// Non-cascading bound Sink (preserves Batcher's documented "drains its own queue but not its
    /// downstream" contract - overrides the cascading default in [Processor#asSink]).
    @Override
    public Sink<T> asSink(Sink<? super T> downstream) {
        bindNext(Objects.requireNonNull(downstream, "downstream"));
        return this;
    }

    private void bindNext(Sink<? super T> forward) {
        var prev = next.compareAndExchange(null, forward);
        if (prev != null && prev != forward) {
            throw new IllegalStateException(
                    "Batcher bound to two downstreams; one Batcher instance serves one chain");
        }
    }

    private CompletionStage<ConsumeResult> enqueue(T batch) {
        if (closed.get()) {
            return CompletableFuture.completedFuture(ConsumeResult.retryable("batcher closed"));
        }
        // 0 is the healthy sentinel (nanoTime has an arbitrary origin, so compare relatively).
        var degradedUntil = degradedUntilNanos;
        if (reflectDownstreamFailures && degradedUntil != 0L && System.nanoTime() - degradedUntil < 0) {
            return CompletableFuture.completedFuture(ConsumeResult.throttled(
                    flushInterval, "batcher degraded: last flush failed", lastDrainFailure));
        }

        var ack = ackMode == AckMode.ON_DELIVERY ? new CompletableFuture<ConsumeResult>() : null;
        var entry = new QueueEntry<>(batch, ack);

        if (!queue.offer(entry)) {
            switch (overflowPolicy) {
                case DROP_OLDEST -> {
                    // Evict oldest entries until the arriving one is admitted. A single poll frees
                    // exactly one slot, but a concurrent producer can take it before our re-offer, so
                    // loop rather than offer once: otherwise the arriving entry never enters the queue
                    // and, under ON_DELIVERY, its caller's ack is stranded (never drained, evicted,
                    // reclaimed, or swept) and the loss goes uncounted.
                    while (!queue.offer(entry)) {
                        var evicted = queue.poll();
                        if (evicted != null) {
                            drops.increment();
                            // ON_DELIVERY: the evicted caller is still waiting - tell it, retryably.
                            // ON_ENQUEUE: evicted.ack is null -> completeAck is a no-op.
                            completeAck(evicted, ConsumeResult.throttled(
                                    flushInterval, "batcher evicted an older batch under DROP_OLDEST"));
                        }
                    }
                }
                case DROP_NEWEST -> {   // arriving batch never entered the queue; its fresh ack is unused
                    drops.increment();
                    var rejectedItems = signal.itemCount(batch);
                    return CompletableFuture.completedFuture(rejectedItems == 0
                            ? ConsumeResult.accepted()
                            : ConsumeResult.partial(rejectedItems, "batcher queue full"));
                }
                case BLOCK -> {
                    try {
                        queue.put(entry);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return CompletableFuture.completedFuture(
                                ConsumeResult.permanent("interrupted waiting for batcher capacity", e));
                    }
                }
                case FAIL -> {
                    drops.increment();
                    return CompletableFuture.completedFuture(
                            ConsumeResult.throttled(flushInterval, "batcher queue full"));
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
        // Shutdown may have flipped closed after the initial check; reclaim by IDENTITY to avoid stranding.
        if (closed.get() && queue.remove(entry)) {
            var r = ConsumeResult.retryable("batcher closed");
            completeAck(entry, r);
            return CompletableFuture.completedFuture(r);
        }
        return ack != null ? ack : ConsumeResult.acceptedStage();
    }

    /// Completes an entry's ack (a no-op in [AckMode#ON_ENQUEUE], where the ack is null).
    /// `complete` is at-most-once, the guard against double-completion races.
    private void completeAck(QueueEntry<T> entry, ConsumeResult result) {
        if (entry.ack != null) {
            entry.ack.complete(result);
        }
    }

    /// The number of batches dropped by queue overflow (never enqueued or evicted to make room),
    /// per the [OverflowPolicy]. Distinct from [#drainFailedCount]. Monotone, thread-safe.
    ///
    /// @return the number of batches dropped by overflow since construction
    public long droppedCount() {
        return drops.sum();
    }

    /// Count of batches that were part of a failed drain - an impossible merge, a downstream that
    /// threw or completed exceptionally, or a downstream that returned Partial/Rejected. A pure
    /// drain-health signal; its data-loss meaning depends on the ack mode:
    ///
    ///   - under [AckMode#ON_ENQUEUE] these batches are SILENTLY LOST (the caller was already told
    ///     Accepted at enqueue); total silent loss there is [#droppedCount] + `drainFailedCount()`.
    ///   - under the default [AckMode#ON_DELIVERY] these batches are NOT lost - every caller was told
    ///     the real result and can retain and retry - so this counter reads as "downstream delivery
    ///     failures observed", not "data lost".
    ///
    /// Distinct from [#droppedCount] (queue overflow). Monotone, thread-safe.
    ///
    /// @return the number of batches involved in failed drains since construction
    public long drainFailedCount() {
        return drainFailed.sum();
    }

    /// The number of batches currently buffered, awaiting a drain.
    ///
    /// @return the current queue depth
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
                    // Sweep any entries offered during the shutdown race so no ON_DELIVERY caller
                    // hangs forever; complete them retryably.
                    sweepResidualAcks();
                    drainExecutor.shutdown();
                });
    }

    /// Completes the acks of any entries still queued after the final drain (shutdown-race
    /// stragglers), retryably, so no [AckMode#ON_DELIVERY] caller is stranded.
    private void sweepResidualAcks() {
        var residual = new ArrayList<QueueEntry<T>>(queue.size());
        queue.drainTo(residual);
        for (var entry : residual) {
            completeAck(entry, ConsumeResult.retryable("batcher shut down before delivery"));
        }
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
        private final Signal<T> signal;
        private @Nullable Sink<? super T> downstream;
        private int flushThreshold = 512;
        private int queueCapacity = 2048;
        private OverflowPolicy overflowPolicy = OverflowPolicy.DROP_NEWEST;
        private boolean reflectDownstreamFailures = false;
        private AckMode ackMode = AckMode.ON_DELIVERY;
        private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
        private @Nullable Duration deliveryTimeout = null;
        private @Nullable BatchScheduler scheduler = null;

        private Builder(Signal<T> signal) {
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

        /// Selects when `consume`'s ack completes: [AckMode#ON_DELIVERY] (the default, ack after the
        /// batch is delivered downstream and carrying the real result) or [AckMode#ON_ENQUEUE] (ack
        /// `Accepted` at enqueue, before delivery, lossy on a later drain failure).
        ///
        /// @param mode the ack mode
        /// @return this builder
        public Builder<T> ackMode(AckMode mode) {
            this.ackMode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        /// Sets the drain-timer interval (default one second): the period of the age-based flush and
        /// the retry-after hint on a [OverflowPolicy#FAIL] or degraded-ingress rejection. Under
        /// [AckMode#ON_DELIVERY] it also bounds a sub-threshold batch's ack latency to first drain.
        ///
        /// @param interval the flush interval, must be positive
        /// @return this builder
        public Builder<T> flushInterval(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("flushInterval must be > 0");
            }
            this.flushInterval = interval;
            return this;
        }

        /// Bounds the in-flight downstream delivery leg (default unset, unbounded). When set, a
        /// downstream stage that does not complete within `timeout` fails the drain, so no
        /// [AckMode#ON_DELIVERY] caller's ack is stranded by a hung custom downstream. The bundled
        /// transports complete on a per-call virtual thread and do not need it; a forwarding gateway
        /// with a custom downstream should set it at or below the transport deadline.
        ///
        /// @param timeout the maximum time to await a downstream delivery, must be positive
        /// @return this builder
        public Builder<T> deliveryTimeout(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("deliveryTimeout must be > 0");
            }
            this.deliveryTimeout = timeout;
            return this;
        }

        /// Sets the [BatchScheduler] whose daemon thread drives this batcher's age-based flush timer
        /// (default: the process-wide [BatchScheduler#shared] fallback). The batcher **uses** the
        /// scheduler but never owns or closes it: register it with
        /// [dev.nthings.otlp4j.pipeline.Wiring.Builder#owns] (a [BatchScheduler] is [AutoCloseable])
        /// or close it by hand, in both cases only **after** shutting the batcher down. Share one
        /// scheduler across a gateway's batchers to isolate its timer thread from other gateways in
        /// the same JVM.
        ///
        /// @param scheduler the scheduler to drive the flush timer
        /// @return this builder
        public Builder<T> scheduler(BatchScheduler scheduler) {
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
            return this;
        }

        /// Builds the batcher. If [#downstream(Sink)] was set, the result is the bound Sink/terminal
        /// form; otherwise it is the mid-chain [Processor] form, capturing its `next` from the pipeline
        /// when attached via `Stage.via(...)` (using it as a bare Sink without a bound downstream throws).
        public Batcher<T> build() {
            return new Batcher<>(signal, this);
        }

        /// Builds the mid-chain [Processor] form: sugar for [#build()] with no downstream, reading as
        /// "mid-chain form" at the call site. Attach the result via `Stage.via(...)`. Because this form
        /// captures its `next` from the pipeline, a downstream must not have been bound; calling
        /// `buildProcessor()` after [#downstream(Sink)] is a contradiction and throws
        /// [IllegalStateException] (use [#build()] for the bound Sink/terminal form).
        public Batcher<T> buildProcessor() {
            if (downstream != null) {
                throw new IllegalStateException(
                        "buildProcessor() builds the unbound mid-chain form, but a downstream was set; "
                        + "use build() for the bound Sink/terminal form, or drop .downstream(...) for the "
                        + "mid-chain form");
            }
            return build();
        }
    }
}
