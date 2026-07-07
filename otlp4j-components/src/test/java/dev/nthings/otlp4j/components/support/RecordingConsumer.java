package dev.nthings.otlp4j.components.support;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/// A programmable downstream [SignalConsumer] test double: it records every payload it is handed, can
/// gate its `consume` on a latch to hold a delivery worker while a queue fills, and returns a
/// programmable [DeliveryResult]. Its default result is acceptance.
public final class RecordingConsumer implements SignalConsumer<TelemetryPayload> {

    private final Provenance provenance =
            new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);
    private final List<TelemetryPayload> received = new CopyOnWriteArrayList<>();
    private final AtomicInteger completions = new AtomicInteger();

    private volatile @Nullable CountDownLatch gate;
    private volatile Function<TelemetryPayload, DeliveryResult> resultFn = payload -> DeliveryResult.accepted(provenance);

    /// Programs the result this consumer returns for every payload.
    ///
    /// @param resultFn the result function, applied per payload
    public void result(Function<TelemetryPayload, DeliveryResult> resultFn) {
        this.resultFn = resultFn;
    }

    /// Programs this consumer to always reject with the given result.
    ///
    /// @param result the rejection result to return
    public void rejectWith(DeliveryResult result) {
        this.resultFn = payload -> result;
    }

    /// Programs this consumer to throw from `consume`, exercising the drain failure path.
    public void throwOnConsume() {
        this.resultFn = payload -> {
            throw new IllegalStateException("programmed drain failure");
        };
    }

    /// Gates `consume` on `latch`: each delivery blocks until the latch is released. Passing null
    /// clears the gate.
    ///
    /// @param latch the gate latch, or null to clear
    public void gate(@Nullable CountDownLatch latch) {
        this.gate = latch;
    }

    /// The payloads received, in arrival order.
    ///
    /// @return the received payloads
    public List<TelemetryPayload> received() {
        return List.copyOf(received);
    }

    /// The number of completed deliveries (past any gate).
    ///
    /// @return the completion count
    public int completions() {
        return completions.get();
    }

    /// The total OTLP item count across every received payload.
    ///
    /// @return the total item count
    public long totalItems() {
        return received.stream().mapToLong(TelemetryPayload::itemCount).sum();
    }

    /// The provenance this consumer stamps onto its default acceptance result.
    ///
    /// @return the provenance
    public Provenance provenance() {
        return provenance;
    }

    @Override
    public ConsumerCapabilities capabilities() {
        return ConsumerCapabilities.nonMutating();
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
        received.add(data);
        var latch = gate;
        if (latch != null) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        var result = resultFn.apply(data);
        completions.incrementAndGet();
        return CompletableFuture.completedFuture(result);
    }
}
