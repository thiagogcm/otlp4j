package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.components.internal.MatchRule;
import dev.nthings.otlp4j.components.internal.MatchSpec;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.config.ConfigBinder;
import dev.nthings.otlp4j.core.config.ConfigNode;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// The factory for the built-in routing processor, type `routing`, stability `ALPHA`.
///
/// It supports every signal, for the same reason the filter processor does: it reads only the
/// routing-metadata and client-metadata axes (or, through the escape hatch, the domain). It implements
/// [ConfigBinder], reading the same four condition maps [FilterProcessorFactory] does
/// (`resource_attribute`, `resource_attribute_regex`, `header`, `header_regex`) directly under this
/// component's own configuration, plus a `default_route` boolean.
public final class RoutingProcessorFactory implements ProcessorFactory<RoutingConfig>, ConfigBinder<RoutingConfig> {

    /// The component type name.
    public static final String TYPE = "routing";

    /// Creates a routing processor factory.
    public RoutingProcessorFactory() {
    }

    @Override
    public ComponentKind kind() {
        return ComponentKind.PROCESSOR;
    }

    @Override
    public StabilityLevel stability() {
        return StabilityLevel.ALPHA;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public RoutingConfig defaultConfig() {
        return RoutingConfig.defaults();
    }

    @Override
    public <T extends TelemetryPayload> Processor<T> create(
            CreateContext ctx, RoutingConfig config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
        Processor<TelemetryPayload> processor = new RoutingProcessor(
                ctx.id(), signal, config.match(), config.defaultRoute(), payloadConsumer(next));
        return castProcessor(processor);
    }

    @Override
    public RoutingConfig bind(ConfigNode node, RoutingConfig defaults) {
        var match = MatchRule.fromConfig(node).map(MatchSpec::of).orElse(defaults.match());
        var defaultRoute = node.bool("default_route", defaults.defaultRoute());
        return new RoutingConfig(match, defaultRoute);
    }

    @SuppressWarnings("unchecked")
    private static SignalConsumer<TelemetryPayload> payloadConsumer(SignalConsumer<?> next) {
        return (SignalConsumer<TelemetryPayload>) next;
    }

    @SuppressWarnings("unchecked")
    private static <T extends TelemetryPayload> Processor<T> castProcessor(Processor<TelemetryPayload> processor) {
        return (Processor<T>) (Processor<?>) processor;
    }
}
