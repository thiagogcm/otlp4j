package dev.nthings.otlp4j.service.internal;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.component.SignalSupport;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.time.Duration;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/// A socket-free receiver. The monitor linearizes admission, barriers and shutdown.
/// @param <T> the domain batch type
public final class ApplicationInput<T> implements Receiver {
    private final SignalDescriptor<T> signal;
    private final SignalConsumer<T> downstream;
    private final Provenance provenance;
    private final Object owner;
    private boolean accepting;
    private final Set<CompletableFuture<Void>> pending = new HashSet<>();

    private ApplicationInput(SignalDescriptor<T> signal, SignalConsumer<T> downstream,
                             ComponentId id, Object owner) {
        this.signal = signal;
        this.downstream = downstream;
        this.provenance = new Provenance(id, ComponentKind.RECEIVER, signal);
        this.owner = owner;
    }

    public boolean belongsTo(Object expected) { return owner == expected; }

    public CompletionStage<DeliveryResult> submit(T batch, ConsumeContext context) {
        Objects.requireNonNull(context, "context");
        var payload = Payload.of(signal, batch);
        var completion = new CompletableFuture<Void>();
        synchronized (this) {
            if (!accepting) {
                return CompletableFuture.completedFuture(
                        DeliveryResult.shuttingDown("application input is not running", provenance));
            }
            if (context.isCancelled()) {
                return CompletableFuture.completedFuture(
                        DeliveryResult.permanent("submission is cancelled", provenance));
            }
            pending.add(completion);
        }
        CompletionStage<DeliveryResult> delivery;
        try {
            delivery = GraphNodeBoundary.deliver(downstream, provenance, context, payload);
        } catch (Throwable failure) {
            settled(completion);
            throw failure;
        }
        return delivery.whenComplete((result, failure) -> settled(completion));
    }

    private void settled(CompletableFuture<Void> completion) {
        synchronized (this) {
            pending.remove(completion);
        }
        completion.complete(null);
    }

    @Override
    public synchronized CompletionStage<Void> start(ComponentContext context) {
        accepting = true;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<FlushReport> flush(FlushContext context) {
        return barrier(context.deadline()).thenApply(ignored -> FlushReport.empty());
    }

    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        synchronized (this) { accepting = false; }
        return barrier(context.deadline());
    }

    private synchronized CompletionStage<Void> barrier(Duration timeout) {
        return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
                .orTimeout(TimeUnit.NANOSECONDS.convert(timeout), TimeUnit.NANOSECONDS);
    }

    /// Captures the input definition; each graph build gets an independent runtime instance.
    public static final class Factory<T> implements ReceiverFactory<ComponentConfig.None>, SignalSupport {
        private final SignalDescriptor<T> signal;
        private final Object owner;

        public Factory(SignalDescriptor<T> signal, Object owner) {
            this.signal = Objects.requireNonNull(signal, "signal");
            this.owner = owner;
        }

        @Override public String type() { return "input"; }
        @Override public ComponentKind kind() { return ComponentKind.RECEIVER; }
        @Override public ComponentConfig.None defaultConfig() { return ComponentConfig.NONE; }
        @Override public boolean supportsSignal(SignalDescriptor<?> candidate) { return signal == candidate; }
        @Override public Receiver create(CreateContext context, ComponentConfig.None config, SignalConsumers consumers) {
            return new ApplicationInput<>(signal, consumers.consumer(signal), context.id(), owner);
        }
    }
}
