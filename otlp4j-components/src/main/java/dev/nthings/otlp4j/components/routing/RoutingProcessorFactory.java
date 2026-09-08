package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// The factory for the built-in routing processor, type `routing`.
///
/// It supports every signal, for the same reason the filter processor does: it reads only the
/// routing-metadata and client-metadata axes (or, through the escape hatch, the domain).
public final class RoutingProcessorFactory implements ProcessorFactory<RoutingConfig> {

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
    public String type() {
        return TYPE;
    }

    @Override
    public RoutingConfig defaultConfig() {
        return RoutingConfig.defaults();
    }

    @Override
    public <T> Processor<T> create(
            CreateContext ctx, RoutingConfig config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
        return new RoutingProcessor<>(ctx.id(), signal, config.match(), next);
    }
}
