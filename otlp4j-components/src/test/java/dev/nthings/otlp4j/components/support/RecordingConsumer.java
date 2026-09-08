package dev.nthings.otlp4j.components.support;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/// A programmable downstream consumer test double. It records received payloads, optionally gates
/// deliveries on a latch, and returns a programmable result.
public final class RecordingConsumer<T> implements SignalConsumer<T> {

    private final Provenance provenance =
            new Provenance(ComponentId.parse("sink"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);
    private final List<Payload<T>> received = new CopyOnWriteArrayList<>();
    private final List<ConsumeContext> contexts = new CopyOnWriteArrayList<>();
    private final AtomicInteger completions = new AtomicInteger();

    private volatile @Nullable CountDownLatch gate;
    private volatile Function<Payload<T>, DeliveryResult> resultFn = payload -> DeliveryResult.accepted(provenance);

    public void result(Function<Payload<T>, DeliveryResult> resultFn) {
        this.resultFn = resultFn;
    }

    public void rejectWith(DeliveryResult result) {
        this.resultFn = payload -> result;
    }

    public void throwOnConsume() {
        this.resultFn = payload -> {
            throw new IllegalStateException("programmed drain failure");
        };
    }

    public void gate(@Nullable CountDownLatch latch) {
        this.gate = latch;
    }

    public List<Payload<T>> received() {
        return List.copyOf(received);
    }

    public List<ConsumeContext> contexts() {
        return List.copyOf(contexts);
    }

    public int completions() {
        return completions.get();
    }

    public long totalItems() {
        return received.stream().mapToLong(Payload::itemCount).sum();
    }

    public Provenance provenance() {
        return provenance;
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
        contexts.add(context);
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
