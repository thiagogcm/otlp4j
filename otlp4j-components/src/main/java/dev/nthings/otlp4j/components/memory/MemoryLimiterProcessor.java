package dev.nthings.otlp4j.components.memory;

import dev.nthings.otlp4j.components.internal.MemoryLimiter;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/// The built-in memory limiter processor. It samples a used-memory gauge every check interval through a
/// shared [MemoryLimiter] core and reacts to the sampled state on the consume path: in the hard state it
/// refuses ingress with a retryable throttled result carrying a retry-after hint, so the receiver emits
/// gRPC pushback or an HTTP `Retry-After` header end to end; in the soft state it requests a best-effort
/// collection no more than once per check interval and forwards the payload; in the normal state it
/// forwards unchanged. Recovery is automatic: once a later sample reads used memory back under the soft
/// threshold the limiter returns to normal and payloads are admitted again.
///
/// The processor is non-mutating: it inspects nothing in the payload and forwards the input identity
/// untouched, so it supports every signal.
public final class MemoryLimiterProcessor implements Processor<TelemetryPayload> {

    private final Provenance provenance;
    private final MemoryLimiter limiter;
    private final long checkIntervalNanos;
    private final Duration retryAfter;
    private final SignalConsumer<TelemetryPayload> next;
    private final LongSupplier nanoTime;
    private final Runnable collect;

    private final AtomicLong lastCollectNanos;
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();

    private volatile boolean closed;
    private volatile @Nullable ScheduledExecutorService scheduler;

    /// Builds a memory limiter processor over the shared limiter core.
    ///
    /// @param id            the component identity
    /// @param signal        the signal this processor forwards, used only to stamp provenance
    /// @param limiter       the shared limiter core reading the used-memory gauge
    /// @param checkInterval how often the gauge is sampled and the collection throttle resets
    /// @param retryAfter    the retry-after hint on a hard-limit refusal
    /// @param next          the downstream consumer
    /// @param nanoTime      the monotonic nanosecond source the collection throttle reads
    /// @param collect       the best-effort collection action requested in the soft state
    MemoryLimiterProcessor(
            ComponentId id,
            SignalDescriptor<?> signal,
            MemoryLimiter limiter,
            Duration checkInterval,
            Duration retryAfter,
            SignalConsumer<TelemetryPayload> next,
            LongSupplier nanoTime,
            Runnable collect) {
        this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
        this.limiter = limiter;
        this.checkIntervalNanos = checkInterval.toNanos();
        this.retryAfter = retryAfter;
        this.next = next;
        this.nanoTime = nanoTime;
        this.collect = collect;
        // Seed the throttle one interval in the past so the first soft-state consume may collect.
        this.lastCollectNanos = new AtomicLong(nanoTime.getAsLong() - checkIntervalNanos);
    }

    @Override
    public ConsumerCapabilities capabilities() {
        return ConsumerCapabilities.nonMutating();
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
        if (closed) {
            return CompletableFuture.completedFuture(
                    DeliveryResult.shuttingDown("memory limiter is shutting down", provenance));
        }
        return switch (limiter.state()) {
            case HARD -> CompletableFuture.completedFuture(
                    DeliveryResult.throttled(retryAfter, "memory hard limit exceeded", provenance));
            case SOFT -> {
                maybeCollect();
                yield next.consume(context, data);
            }
            case NORMAL -> next.consume(context, data);
        };
    }

    /// Requests a best-effort collection at most once per check interval, so a sustained soft state
    /// does not collect on every payload.
    private void maybeCollect() {
        var now = nanoTime.getAsLong();
        var last = lastCollectNanos.get();
        if (now - last >= checkIntervalNanos && lastCollectNanos.compareAndSet(last, now)) {
            collect.run();
        }
    }

    @Override
    public CompletionStage<Void> start(ComponentContext context) {
        var exec = Executors.newSingleThreadScheduledExecutor(runnable -> {
            var thread = new Thread(runnable, "memory-limiter-" + provenance.componentId());
            thread.setDaemon(true);
            return thread;
        });
        // Sample once immediately so the state is accurate before the first payload, then periodically.
        limiter.refresh();
        exec.scheduleAtFixedRate(limiter::refresh, checkIntervalNanos, checkIntervalNanos, TimeUnit.NANOSECONDS);
        this.scheduler = exec;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        closed = true;
        var exec = scheduler;
        if (exec != null) {
            exec.shutdownNow();
        }
        return CompletableFuture.completedFuture(null);
    }
}
