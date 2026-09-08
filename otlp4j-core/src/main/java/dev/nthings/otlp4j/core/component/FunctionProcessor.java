package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// Package-private implementation behind the typed function processor factories.
final class FunctionProcessor<M> implements Processor<M> {

    private final UnaryOperator<M> mapper;
    private final Predicate<M> predicate;
    private final SignalConsumer<M> next;
    private final Provenance provenance;

    FunctionProcessor(
            ComponentId id, SignalDescriptor<M> signal, UnaryOperator<M> mapper,
            Predicate<M> predicate, SignalConsumer<M> next) {
        this.mapper = mapper;
        this.predicate = predicate;
        this.next = Objects.requireNonNull(next, "next");
        this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<M> data) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(data, "data");
        var domain = data.data();
        if (predicate != null) {
            return predicate.test(domain)
                    ? next.consume(context, data)
                    : DeliveryResult.acceptedStage(provenance);
        }
        var transformed = Objects.requireNonNull(mapper.apply(domain), "mapper returned null");
        return next.consume(context, data.withData(transformed));
    }

    @Override
    public CompletionStage<Void> start(ComponentContext context) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        return CompletableFuture.completedFuture(null);
    }
}
