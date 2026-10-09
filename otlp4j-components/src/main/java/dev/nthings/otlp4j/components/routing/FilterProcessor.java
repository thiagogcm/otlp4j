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
import dev.nthings.otlp4j.model.Attributes;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/// The built-in filter processor. It keeps the resource groups of a payload that match
/// [FilterConfig#include] and do not match [FilterConfig#exclude], each with all its items, and
/// forwards them to the next consumer. A payload whose resource groups are all kept is forwarded
/// unchanged; one whose groups are all rejected is not forwarded and reports an accepted delivery; a
/// mixed payload is forwarded with only its kept groups.
///
/// Several pipelines that share a source and each start with a filter route a stream by resource
/// attribute or header: each branch receives exactly the resource groups its own condition selects.
///
/// Deciding never materialises the domain batch unless a rule carries the domain-predicate escape
/// hatch. Only a mixed payload is decoded, to drop its rejected groups. The filter supports every
/// signal.
public final class FilterProcessor<T> implements Processor<T> {

    private final SignalDescriptor<T> signal;
    private final Provenance provenance;
    private final MatchSpec include;
    private final MatchSpec exclude;
    private final SignalConsumer<T> next;

    FilterProcessor(
            ComponentId id, SignalDescriptor<T> signal, MatchSpec include, MatchSpec exclude,
            SignalConsumer<T> next) {
        this.signal = signal;
        this.provenance = new Provenance(id, ComponentKind.PROCESSOR, signal);
        this.include = include;
        this.exclude = exclude;
        this.next = next;
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
        var included = include.resourceMatcher(data, context);
        var excluded = exclude.resourceMatcher(data, context);
        Predicate<Attributes> keep = resource -> included.test(resource) && !excluded.test(resource);

        var resources = data.routingMetadata().resources();
        if (resources.isEmpty()) {
            // Without a resource group no resource condition can hold; header and domain conditions
            // still decide whether the payload passes.
            return keep.test(Attributes.empty())
                    ? next.consume(context, data)
                    : DeliveryResult.acceptedStage(provenance);
        }
        var kept = 0;
        for (var resource : resources) {
            if (keep.test(resource)) {
                kept++;
            }
        }
        if (kept == 0) {
            return DeliveryResult.acceptedStage(provenance);
        }
        if (kept == resources.size()) {
            return next.consume(context, data);
        }
        var selected = data.withData(
                signal.filterResources(data.data(), resource -> keep.test(resource.attributes())));
        // The decision above read routing metadata; the selection read the decoded batch. A payload
        // whose two views disagree must still never be forwarded once nothing is left of it.
        return selected.routingMetadata().resources().isEmpty()
                ? DeliveryResult.acceptedStage(provenance)
                : next.consume(context, selected);
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
