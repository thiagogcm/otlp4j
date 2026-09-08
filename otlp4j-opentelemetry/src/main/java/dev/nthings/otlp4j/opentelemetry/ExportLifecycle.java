package dev.nthings.otlp4j.opentelemetry;

import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import io.opentelemetry.sdk.common.CompletableResultCode;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Shared lifecycle and result handling for the three SDK exporter adapters. */
abstract class ExportLifecycle<T> {

    static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration INPUT_FLUSH_TIMEOUT = Duration.ofSeconds(10);

    private final Object lifecycleLock = new Object();
    private final TelemetryInput<T> input;
    private final Duration shutdownTimeout;
    private State state = State.OPEN;
    private int inFlight;
    private boolean drainStarted;
    private CompletableResultCode shutdownResult;
    private long shutdownDeadlineNanos;

    ExportLifecycle(TelemetryInput<T> input) {
        this(input, DEFAULT_SHUTDOWN_TIMEOUT);
    }

    ExportLifecycle(TelemetryInput<T> input, Duration shutdownTimeout) {
        this.input = Objects.requireNonNull(input, "input");
        this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
        if (shutdownTimeout.isNegative() || shutdownTimeout.isZero()) {
            throw new IllegalArgumentException("shutdownTimeout must be > 0");
        }
    }

    protected final TelemetryInput<T> input() {
        return input;
    }

    /** Returns success for an empty SDK export while still enforcing adapter lifecycle. */
    protected final CompletableResultCode emptyExport() {
        synchronized (lifecycleLock) {
            return state == State.OPEN
                    ? CompletableResultCode.ofSuccess()
                    : CompletableResultCode.ofFailure();
        }
    }

    /** Starts one export operation, translating its terminal delivery result to SDK semantics. */
    protected final CompletableResultCode submit(
            Supplier<CompletionStage<DeliveryResult>> operation) {
        if (!beginOperation()) {
            return CompletableResultCode.ofFailure();
        }
        var result = new CompletableResultCode();
        CompletionStage<DeliveryResult> stage;
        try {
            stage = Objects.requireNonNull(operation.get(), "input.submit returned null");
        } catch (RuntimeException exception) {
            finishOperation();
            return result.failExceptionally(exception);
        }
        try {
            stage.whenComplete((delivery, failure) -> {
                try {
                    if (failure != null) {
                        result.failExceptionally(failure);
                    } else if (delivery instanceof DeliveryResult.Accepted
                            || delivery instanceof DeliveryResult.Partial) {
                        result.succeed();
                    } else {
                        result.fail();
                    }
                } finally {
                    finishOperation();
                }
            });
        } catch (RuntimeException exception) {
            finishOperation();
            return result.failExceptionally(exception);
        }
        return result;
    }

    /** Flushes accepted input work and translates aggregate loss/failure to SDK success. */
    protected final CompletableResultCode flushInput() {
        if (!beginOperation()) {
            return CompletableResultCode.ofFailure();
        }
        var result = new CompletableResultCode();
        CompletionStage<FlushReport> stage;
        try {
            stage = Objects.requireNonNull(
                    input.flush(INPUT_FLUSH_TIMEOUT), "input.flush returned null");
        } catch (RuntimeException exception) {
            finishOperation();
            return result.failExceptionally(exception);
        }
        try {
            stage.whenComplete((report, failure) -> {
                try {
                    if (failure != null) {
                        result.failExceptionally(failure);
                    } else if (report != null && report.isSuccess()) {
                        result.succeed();
                    } else {
                        result.fail();
                    }
                } finally {
                    finishOperation();
                }
            });
        } catch (RuntimeException exception) {
            finishOperation();
            return result.failExceptionally(exception);
        }
        return result;
    }

    /** Shuts down only this adapter, draining accepted work from the borrowed input. */
    protected final CompletableResultCode shutdownAdapter() {
        boolean startDrain = false;
        synchronized (lifecycleLock) {
            if (state == State.CLOSED || state == State.CLOSING) {
                return shutdownResult == null ? CompletableResultCode.ofSuccess() : shutdownResult;
            }
            state = State.CLOSING;
            shutdownResult = new CompletableResultCode();
            shutdownDeadlineNanos = deadlineNanos(shutdownTimeout);
            if (inFlight == 0) {
                drainStarted = true;
                startDrain = true;
            }
        }
        scheduleShutdownTimeout();
        if (startDrain) {
            drainAfterShutdownRequested();
        }
        return shutdownResult;
    }

    private boolean beginOperation() {
        synchronized (lifecycleLock) {
            if (state != State.OPEN) {
                return false;
            }
            inFlight++;
            return true;
        }
    }

    private void finishOperation() {
        boolean startDrain = false;
        synchronized (lifecycleLock) {
            inFlight--;
            if (state == State.CLOSING && inFlight == 0 && !drainStarted) {
                drainStarted = true;
                startDrain = true;
            }
        }
        if (startDrain) {
            drainAfterShutdownRequested();
        }
    }

    private void drainAfterShutdownRequested() {
        // A user-provided input is allowed to implement flush synchronously. Keep that call away
        // from the SDK shutdown caller and the CompletableFuture timer thread so either can still
        // enforce the single monotonic shutdown budget.
        Thread.startVirtualThread(this::drainInputOnVirtualThread);
    }

    private void drainInputOnVirtualThread() {
        var remaining = remainingShutdownTimeout();
        if (remaining.isZero()) {
            shutdownDeadlineReached();
            return;
        }
        CompletionStage<FlushReport> stage;
        try {
            stage = Objects.requireNonNull(input.flush(remaining), "input.flush returned null");
        } catch (RuntimeException exception) {
            completeShutdown(null, exception);
            return;
        }
        try {
            stage.whenComplete(this::completeShutdown);
        } catch (RuntimeException exception) {
            completeShutdown(null, exception);
        }
    }

    private void completeShutdown(FlushReport report, Throwable failure) {
        CompletableResultCode result;
        synchronized (lifecycleLock) {
            if (state != State.CLOSING || shutdownResult == null) {
                return;
            }
            result = shutdownResult;
            state = State.CLOSED;
        }
        if (failure != null) {
            result.failExceptionally(failure);
        } else if (report != null && report.isSuccess()) {
            result.succeed();
        } else {
            result.fail();
        }
    }

    private void scheduleShutdownTimeout() {
        var delayNanos = shutdownDeadlineNanos - System.nanoTime();
        // Run the timeout callback directly on CompletableFuture's daemon timer thread so a
        // saturated common pool cannot extend the adapter's shutdown budget. Dispatch the actual
        // result completion to a virtual thread so SDK callbacks never run on the global timer.
        CompletableFuture.delayedExecutor(delayNanos, TimeUnit.NANOSECONDS, Runnable::run)
                .execute(() -> Thread.startVirtualThread(this::shutdownDeadlineReached));
    }

    private void shutdownDeadlineReached() {
        CompletableResultCode result;
        synchronized (lifecycleLock) {
            if (state != State.CLOSING || shutdownResult == null) {
                return;
            }
            state = State.CLOSED;
            result = shutdownResult;
        }
        result.failExceptionally(new TimeoutException(
                "OpenTelemetry exporter shutdown timed out after " + shutdownTimeout));
    }

    private Duration remainingShutdownTimeout() {
        var remainingNanos = shutdownDeadlineNanos - System.nanoTime();
        return remainingNanos <= 0 ? Duration.ZERO : Duration.ofNanos(remainingNanos);
    }

    private static long deadlineNanos(Duration timeout) {
        return System.nanoTime() + timeout.toNanos();
    }

    private enum State {
        OPEN,
        CLOSING,
        CLOSED
    }
}
