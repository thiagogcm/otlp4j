package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

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
    public <T extends TelemetryPayload> Processor<T> create(
            CreateContext ctx, RoutingConfig config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
        Processor<TelemetryPayload> processor = new RoutingProcessor(
                ctx.id(), signal, config.match(), config.defaultRoute(), payloadConsumer(next));
        return castProcessor(processor);
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
