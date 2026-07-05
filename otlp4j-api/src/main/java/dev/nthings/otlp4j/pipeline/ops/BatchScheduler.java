package dev.nthings.otlp4j.pipeline.ops;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;

/// The timer that drives a [Batcher]'s age-based flush: a typed, closeable wrapper over a
/// single-thread [ScheduledExecutorService] that owns thread naming and the mandatory
/// `setRemoveOnCancelPolicy(true)` (so a per-instance cancelled age-timer leaves the shared queue
/// immediately, keeping batchers that share one scheduler isolated).
///
/// A [Batcher] built with [Batcher.Builder#scheduler] **uses** the supplied scheduler but never
/// owns or closes it: the caller does, typically by registering it with
/// [dev.nthings.otlp4j.pipeline.Wiring.Builder#owns]. A batcher built without one falls back to the
/// process-wide [#shared] singleton.
///
/// > **Ordering contract:** shut a batcher (or its enclosing pipeline) down **before** closing the
/// > scheduler it uses. Closing an owned scheduler only stops age-based flushing, never threshold or
/// > shutdown drains, so no data is lost; but under [AckMode#ON_DELIVERY] a sub-threshold batch's
/// > ack then resolves only once some later drain (threshold, `forceFlush`, or `shutdown`) fires.
/// > [dev.nthings.otlp4j.pipeline.Wiring] enforces this ordering structurally (pipelines drain in
/// > phase two, owned resources close in phase three), so the exposure is a standalone batcher whose
/// > owner closes the scheduler out of order.
public final class BatchScheduler implements AutoCloseable {

    /// Distinct sequence numbers for the [#create()] thread names, keeping every owned scheduler's
    /// thread name disjoint from the [#shared] singleton's `otlp4j-batch-timer`.
    private static final AtomicInteger OWNED_SEQ = new AtomicInteger();

    /// The lazily-created process-wide fallback, published under the class monitor.
    private static volatile @Nullable BatchScheduler defaultInstance;

    private final ScheduledThreadPoolExecutor executor;
    /// The [#shared] singleton sets this so [#close] is a no-op: the fallback thread outlives every
    /// batcher and is never torn down.
    private final boolean shared;

    private BatchScheduler(String threadName, boolean shared) {
        this.executor = new ScheduledThreadPoolExecutor(1,
                Thread.ofPlatform().daemon().name(threadName).factory());
        this.executor.setRemoveOnCancelPolicy(true);
        this.shared = shared;
    }

    /// Creates an owned scheduler with one daemon timer thread, distinctly named
    /// `otlp4j-batch-timer-owned-N` so it never collides with the [#shared] singleton's thread. The
    /// caller owns the returned scheduler and must [#close] it (or register it with
    /// [dev.nthings.otlp4j.pipeline.Wiring.Builder#owns]) after shutting down every batcher using it.
    ///
    /// @return a new owned scheduler
    public static BatchScheduler create() {
        return new BatchScheduler("otlp4j-batch-timer-owned-" + OWNED_SEQ.incrementAndGet(), false);
    }

    /// Creates an owned scheduler whose daemon timer thread carries `threadName`, for per-gateway
    /// isolation and debuggability. The caller owns the returned scheduler (see [#create()]).
    ///
    /// @param threadName the name for this scheduler's daemon timer thread
    /// @return a new owned scheduler
    public static BatchScheduler create(String threadName) {
        return new BatchScheduler(Objects.requireNonNull(threadName, "threadName"), false);
    }

    /// The process-wide fallback used by batchers built without an explicit scheduler: one daemon
    /// thread named `otlp4j-batch-timer`, created on first use and never shut down (a [#close] on it
    /// is a no-op). Prefer an owned [#create()] scheduler registered with
    /// [dev.nthings.otlp4j.pipeline.Wiring.Builder#owns] for many-gateway isolation.
    ///
    /// @return the shared fallback scheduler
    public static BatchScheduler shared() {
        var s = defaultInstance;
        if (s == null) {
            synchronized (BatchScheduler.class) {
                if ((s = defaultInstance) == null) {
                    defaultInstance = s = new BatchScheduler("otlp4j-batch-timer", true);
                }
            }
        }
        return s;
    }

    /// The wrapped executor, handed to a [Batcher]'s drain engine to schedule its age-based flush.
    ScheduledExecutorService executor() {
        return executor;
    }

    /// Closes an owned scheduler, terminating its timer thread. A no-op on the [#shared] singleton,
    /// whose thread is process-wide and never torn down.
    @Override
    public void close() {
        if (!shared) {
            executor.shutdownNow();
        }
    }
}
