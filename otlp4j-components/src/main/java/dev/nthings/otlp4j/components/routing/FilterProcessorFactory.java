package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// The factory for the built-in filter processor, type `filter`.
///
/// It supports every signal, since a filter rule reads only the routing-metadata and client-metadata
/// axes (or, through the escape hatch, the domain), none of which is signal-specific.
public final class FilterProcessorFactory implements ProcessorFactory<FilterConfig> {

    /// The component type name.
    public static final String TYPE = "filter";

    /// Creates a filter processor factory.
    public FilterProcessorFactory() {
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
    public FilterConfig defaultConfig() {
        return FilterConfig.defaults();
    }

    @Override
    public <T extends TelemetryPayload> Processor<T> create(
            CreateContext ctx, FilterConfig config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
        Processor<TelemetryPayload> processor = new FilterProcessor(
                ctx.id(), signal, config.include(), config.exclude(), SignalConsumer.asPayload(next));
        return ProcessorFactory.asTypedProcessor(processor);
    }
}
