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

/// The built-in routing processor: the branch selector for one leg of a content-routing fan-out. It
/// forwards a payload to its single downstream when [RoutingConfig#match] matches. A payload this
/// branch does not forward is reported as an accepted delivery with nothing forwarded,
/// so the fan-out's other branches are unaffected by this branch's own choice; N branch copies of this
/// processor, each with its own rule and all fed by one upstream fan-out, can route a stream by
/// resource attribute or header. Rules must be disjoint when the branches are intended to partition
/// the stream.
///
/// Matching never materialises the domain batch unless the configured rule carries the domain-predicate
/// escape hatch. The processor never mutates the payload it forwards, so it supports every signal.
public final class RoutingProcessor<T> implements Processor<T> {

    private final Provenance provenance;
    private final MatchSpec match;
    private final SignalConsumer<T> next;

    RoutingProcessor(
            ComponentId id, SignalDescriptor<T> signal, MatchSpec match, SignalConsumer<T> next) {
        this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
        this.match = match;
        this.next = next;
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
        var forward = match.matches(data, context);
        if (forward) {
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
