package dev.nthings.otlp4j.core.delivery;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.RedeliveryPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/// Puts a [DeliveryQueue] in front of any exporter, giving it its own queue and, optionally, its own
/// redelivery. This is the per-destination queue the OTLP specification asks of a client that exports
/// to several destinations, and the upstream Collector's exporter queue: a payload is acknowledged once
/// it is in the queue, delivered by a bounded set of workers, and redelivered on a retryable failure
/// until the [RedeliveryPolicy] runs out. A destination that is down therefore stalls only its own
/// queue, never another destination's, and the sender that was acknowledged does not retry and
/// duplicate the payload at the destinations that already took it.
///
/// A full queue under `FAIL` returns a retryable refusal to the caller and records nothing: the sender
/// owns that retry. Data lost after the acknowledgement is reported to the [QueueObserver] and is part
/// of the flush report and of the failure [#shutdown] ends with.
///
/// The wrapped exporter's lifecycle is driven from here: it is started before the queue's workers, and
/// shut down after the queue has drained, within whatever remains of the shutdown budget even when the
/// drain failed or ran out of time. The exporter declares no capability the graph reads beyond being an
/// [Exporter], so there is nothing to pass through.
///
/// @param <T> the signal's domain batch type
public final class QueuedExporter<T> implements Exporter<T> {

    private final Exporter<T> delegate;
    private final DeliveryQueue<T> queue;
    private final QueueObserver observer;
    private final AtomicReference<@Nullable CompletableFuture<Void>> shutdownResult = new AtomicReference<>();
    private final Provenance provenance;

    private volatile GatewayMetrics.@Nullable Registration depth;

    /// Wraps `delegate` in a queue of its own.
    ///
    /// @param delegate   the exporter that delivers each queued payload
    /// @param policy     the queue capacity, consumer count and overflow behaviour
    /// @param redelivery how a retryable failure is redelivered, or null to attempt each payload once
    /// @param provenance the component identity stamped on results the queue produces
    /// @param observer   told of losses, refusals and the queue depth; it must not block
    public QueuedExporter(
            Exporter<T> delegate,
            DeliveryQueuePolicy policy,
            @Nullable RedeliveryPolicy redelivery,
            Provenance provenance,
            QueueObserver observer) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.provenance = Objects.requireNonNull(provenance, "provenance");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.queue = new DeliveryQueue<>(
                Objects.requireNonNull(policy, "policy"), redelivery, delegate, provenance, observer);
    }

    /// Offers the payload to the queue and acknowledges it on admission. Ownership transfers at the
    /// acknowledgement, so the delivery gets the request's metadata but not its lifetime: the request's
    /// deadline and cancellation end with the acknowledgement, while the queued delivery goes on.
    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(data, "data");
        if (context.isCancelled()) {
            return CompletableFuture.completedFuture(DeliveryResult.permanent("submission is cancelled", provenance));
        }
        if (context.deadline().isExpired(Clock.systemUTC())) {
            return CompletableFuture.completedFuture(
                    DeliveryResult.timedOut("submission deadline elapsed", provenance));
        }
        var detached = ConsumeContext.of(context.metadata(), Deadline.never());
        return queue.offer(detached, data).thenApply(result -> {
            if (result instanceof DeliveryResult.Rejected refusal) {
                observer.refused(data.itemCount(), refusal);
            }
            return result;
        });
    }

    @Override
    public CompletionStage<Void> start(ComponentContext context) {
        return delegate.start(context).thenRun(() -> {
            queue.start();
            depth = observer.depth(queue::size);
        });
    }

    /// Flushes the queue, then the wrapped exporter with what remains of the same deadline, and combines
    /// their reports. The order matters: the wrapped exporter has nothing to flush for an entry until
    /// the queue has delivered it.
    @Override
    public CompletionStage<FlushReport> flush(FlushContext context) {
        Objects.requireNonNull(context, "context");
        var startedAt = System.nanoTime();
        return queue.flush(context.deadline()).thenCompose(queueReport ->
                delegate.flush(new FlushContext(remaining(context.deadline(), startedAt)))
                        .thenApply(queueReport::plus));
    }

    /// Drains the queue, then shuts the wrapped exporter down with what remains of the budget, even
    /// when the drain failed or ran out of time, so the destination is always released. The result
    /// fails when the drain did, when anything acknowledged was lost, or when the wrapped exporter's
    /// shutdown failed.
    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        Objects.requireNonNull(context, "context");
        var existing = shutdownResult.get();
        if (existing != null) {
            return existing;
        }
        var terminal = new CompletableFuture<Void>();
        if (!shutdownResult.compareAndSet(null, terminal)) {
            return Objects.requireNonNull(shutdownResult.get());
        }
        var startedAt = System.nanoTime();
        CompletionStage<FlushReport> drained;
        try {
            drained = queue.drainAndStop(context.deadline());
        } catch (Throwable failure) {
            drained = CompletableFuture.failedFuture(failure);
        }
        drained.whenComplete((report, drainFailure) -> {
            CompletionStage<Void> released;
            try {
                // Inside the guard: a gauge that fails to unregister must not leave the destination
                // unreleased or this shutdown forever incomplete.
                var registration = depth;
                if (registration != null) {
                    registration.close();
                }
                released = delegate.shutdown(new ShutdownContext(remaining(context.deadline(), startedAt)));
            } catch (Throwable failure) {
                released = CompletableFuture.failedFuture(failure);
            }
            released.whenComplete((ignored, releaseFailure) -> {
                Throwable failure = drainFailure;
                if (failure == null && report != null && !report.isSuccess()) {
                    var first = report.firstFailure();
                    failure = first != null
                            ? first
                            : new IllegalStateException("exporter queue shutdown lost telemetry");
                }
                if (failure == null) {
                    failure = releaseFailure;
                } else if (releaseFailure != null && releaseFailure != failure) {
                    failure.addSuppressed(releaseFailure);
                }
                if (failure == null) {
                    terminal.complete(null);
                } else {
                    terminal.completeExceptionally(failure);
                }
            });
        });
        return terminal;
    }

    private static Duration remaining(Duration budget, long startedAt) {
        var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
        return budget.compareTo(elapsed) > 0 ? budget.minus(elapsed) : Duration.ZERO;
    }

    @Override
    public String toString() {
        return "QueuedExporter(" + delegate + ")";
    }
}
