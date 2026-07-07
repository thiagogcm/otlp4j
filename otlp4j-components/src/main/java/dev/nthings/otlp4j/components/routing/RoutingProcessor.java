package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.components.internal.MatchSpec;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// The built-in routing processor: the branch selector for one leg of a content-routing fan-out. It
/// forwards a payload to its single downstream when [RoutingConfig#match] matches, or, on the
/// designated default branch ([RoutingConfig#defaultRoute] true) when `match` is empty, unconditionally.
/// A payload this branch does not forward is reported as an accepted delivery with nothing forwarded,
/// so the fan-out's other branches are unaffected by this branch's own choice; N branch copies of this
/// processor, each with its own rule and all fed by one upstream fan-out, together partition a stream
/// by resource attribute or header.
///
/// Matching never materialises the domain batch unless the configured rule carries the domain-predicate
/// escape hatch. The processor never mutates the payload it forwards, so it supports every signal.
public final class RoutingProcessor implements Processor<TelemetryPayload> {

    private final Provenance provenance;
    private final MatchSpec match;
    private final boolean defaultRoute;
    private final SignalConsumer<TelemetryPayload> next;

    RoutingProcessor(
            ComponentId id, SignalDescriptor<?> signal, MatchSpec match, boolean defaultRoute,
            SignalConsumer<TelemetryPayload> next) {
        this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
        this.match = match;
        this.defaultRoute = defaultRoute;
        this.next = next;
    }

    @Override
    public ConsumerCapabilities capabilities() {
        return ConsumerCapabilities.nonMutating();
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
        var forward = match.rules().isEmpty() ? defaultRoute : match.matches(data, context);
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
