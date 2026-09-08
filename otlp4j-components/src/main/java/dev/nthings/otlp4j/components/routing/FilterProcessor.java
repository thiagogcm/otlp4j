package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.Payload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// The built-in filter processor. It keeps a payload, forwarding it unchanged to the next consumer,
/// when the payload matches [FilterConfig#include] and does not match [FilterConfig#exclude];
/// otherwise it drops the payload, reporting the drop as an accepted delivery with nothing forwarded.
///
/// Matching a resource-attribute or client-metadata-header condition never materialises the domain
/// batch; only a rule carrying the domain-predicate escape hatch forces that read. The processor never
/// mutates the payload it forwards, so it supports every signal.
public final class FilterProcessor<T> implements Processor<T> {

    private final Provenance provenance;
    private final MatchSpec include;
    private final MatchSpec exclude;
    private final SignalConsumer<T> next;

    FilterProcessor(
            ComponentId id, SignalDescriptor<T> signal, MatchSpec include, MatchSpec exclude,
            SignalConsumer<T> next) {
        this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
        this.include = include;
        this.exclude = exclude;
        this.next = next;
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
        var included = include.matches(data, context);
        var excluded = !exclude.rules().isEmpty() && exclude.matches(data, context);
        if (included && !excluded) {
            return next.consume(context, data);
        }
        return DeliveryResult.acceptedStage(provenance);
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
