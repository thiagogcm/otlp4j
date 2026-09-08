package dev.nthings.otlp4j.core.consumer;

import java.lang.ref.WeakReference;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Deadline-aware default context used by in-process submissions. */
final class DefaultConsumeContext implements ConsumeContext {

    private static final ScheduledExecutorService DEADLINE_EXECUTOR = deadlineExecutor();

    private static ScheduledExecutorService deadlineExecutor() {
        var executor = new ScheduledThreadPoolExecutor(
                1, Thread.ofPlatform().daemon().name("otlp4j-consume-deadline-", 0).factory());
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private final ClientMetadata metadata;
    private final Deadline deadline;
    private final Clock clock;
    private final Object cancellationLock = new Object();
    private final CopyOnWriteArrayList<Callback> cancellationCallbacks = new CopyOnWriteArrayList<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private boolean monitorStarted;
    private ScheduledFuture<?> deadlineTask;

    DefaultConsumeContext(ClientMetadata metadata, Deadline deadline) {
        this(metadata, deadline, Clock.systemUTC());
    }

    DefaultConsumeContext(ClientMetadata metadata, Deadline deadline, Clock clock) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public ClientMetadata metadata() {
        return metadata;
    }

    @Override
    public Deadline deadline() {
        return deadline;
    }

    @Override
    public boolean isCancelled() {
        if (cancelled.get()) {
            return true;
        }
        var callbacks = expireIfDue();
        invokeCallbacks(callbacks);
        return cancelled.get();
    }

    @Override
    public void onCancel(Runnable callback) {
        var registration = new Callback(Objects.requireNonNull(callback, "callback"));
        List<Callback> callbacks;
        synchronized (cancellationLock) {
            if (cancelled.get()) {
                callbacks = List.of(registration);
            } else {
                cancellationCallbacks.add(registration);
                callbacks = expireIfDueLocked();
                if (!cancelled.get() && callbacks.isEmpty()) {
                    callbacks = scheduleDeadlineLocked();
                }
            }
        }
        invokeCallbacks(callbacks);
    }

    /** Marks this context cancelled when the deadline is reached and returns callbacks to invoke. */
    private List<Callback> expireIfDue() {
        synchronized (cancellationLock) {
            return expireIfDueLocked();
        }
    }

    private List<Callback> expireIfDueLocked() {
        if (cancelled.get() || !deadline.isExpired(clock)) {
            return List.of();
        }
        return cancelLocked();
    }

    private List<Callback> cancelLocked() {
        if (!cancelled.compareAndSet(false, true)) {
            return List.of();
        }
        if (deadlineTask != null) {
            deadlineTask.cancel(false);
            deadlineTask = null;
        }
        var callbacks = new ArrayList<Callback>(cancellationCallbacks);
        cancellationCallbacks.clear();
        return List.copyOf(callbacks);
    }

    private List<Callback> scheduleDeadlineLocked() {
        if (monitorStarted || deadline.at().equals(Instant.MAX)) {
            return List.of();
        }
        monitorStarted = true;
        var delayMillis = delayMillis();
        if (delayMillis <= 0) {
            return expireIfDueLocked();
        }
        // Do not let the process-wide scheduler retain a completed context (and its request
        // metadata/callbacks) for the remainder of a long deadline. The caller's reference keeps
        // the context alive when it still has an interest in cancellation.
        var contextReference = new WeakReference<>(this);
        deadlineTask = DEADLINE_EXECUTOR.schedule(
                () -> {
                    var context = contextReference.get();
                    if (context != null) {
                        context.expireFromTimer();
                    }
                },
                delayMillis,
                TimeUnit.MILLISECONDS);
        return List.of();
    }

    private long delayMillis() {
        try {
            var remaining = Duration.between(clock.instant(), deadline.at());
            if (remaining.isNegative() || remaining.isZero()) {
                return 0L;
            }
            var millis = remaining.toMillis();
            // Round up: scheduling at a truncated millisecond could fire before the deadline and
            // leave a finite context permanently un-cancelled.
            return remaining.minusMillis(millis).isZero() ? millis : Math.addExact(millis, 1L);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private void expireFromTimer() {
        List<Callback> callbacks;
        synchronized (cancellationLock) {
            // A wall-clock adjustment may make the timer fire a little early. Clear the completed
            // task marker and schedule again in that case instead of leaving this context armed
            // forever with monitorStarted=true.
            deadlineTask = null;
            monitorStarted = false;
            callbacks = expireIfDueLocked();
            if (!cancelled.get()) {
                callbacks = callbacks.isEmpty() ? scheduleDeadlineLocked() : callbacks;
            }
        }
        if (callbacks.isEmpty()) {
            return;
        }
        // Cancellation callbacks are user code. A blocked callback must not stop the single timer
        // thread from cancelling unrelated contexts.
        var callbacksToInvoke = callbacks;
        Thread.startVirtualThread(() -> {
            try {
                invokeCallbacks(callbacksToInvoke);
            } catch (RuntimeException ignored) {
                // Asynchronous callback failures have no registering caller to report to. Every
                // callback has already been attempted by invokeCallbacks before the first failure
                // is discarded here.
            }
        });
    }

    /** Invokes every registration exactly once, retaining the first callback failure until all run. */
    private static void invokeCallbacks(List<Callback> callbacks) {
        RuntimeException firstFailure = null;
        for (var callback : callbacks) {
            try {
                callback.runOnce();
            } catch (RuntimeException failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private static final class Callback {

        private final Runnable action;
        private final AtomicBoolean invoked = new AtomicBoolean();

        private Callback(Runnable action) {
            this.action = action;
        }

        private void runOnce() {
            if (invoked.compareAndSet(false, true)) {
                action.run();
            }
        }
    }
}
