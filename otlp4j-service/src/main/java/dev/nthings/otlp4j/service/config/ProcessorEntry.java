package dev.nthings.otlp4j.service.config;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// A processor's declarative entry in a [GatewayConfig]: the factory paired with its typed
/// configuration. The entry hides the factory's config type parameter behind [#create], which
/// captures it internally so the graph builder wires the processor to its signal and downstream
/// consumer without an unchecked cast.
///
/// @param <C>     the typed configuration this processor's factory accepts
/// @param factory the factory that produces the processor
/// @param config  the configuration the processor is created from
public record ProcessorEntry<C extends ComponentConfig>(ProcessorFactory<C> factory, C config) {

    /// Validates that neither component is null.
    public ProcessorEntry {
        if (factory == null) {
            throw new IllegalArgumentException("processor factory must be non-null");
        }
        if (config == null) {
            throw new IllegalArgumentException("processor config must be non-null");
        }
    }

    /// Creates the processor for `signal`, forwarding to `next`, capturing the factory's config type
    /// internally.
    ///
    /// @param ctx    the create context (identity and base context)
    /// @param signal the signal this processor operates on
    /// @param next   the downstream consumer this processor forwards to
    /// @param <T>    the payload type
    /// @return the processor
    public <T extends TelemetryPayload> Processor<T> create(
            CreateContext ctx, SignalDescriptor<T> signal, SignalConsumer<T> next) {
        return factory.create(ctx, config, signal, next);
    }
}
