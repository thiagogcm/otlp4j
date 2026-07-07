package dev.nthings.otlp4j.service.config;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;

/// A receiver's declarative entry in a [GatewayConfig]: the factory that produces the receiver
/// paired with the typed configuration it is created from. Pairing the factory with its own config
/// type keeps the config checked against its factory at compile time, so no untyped property map
/// crosses the boundary. The entry hides the factory's type parameter behind [#create], which
/// captures it internally so the graph builder never performs an unchecked cast.
///
/// @param <C>     the typed configuration this receiver's factory accepts
/// @param factory the factory that produces the receiver
/// @param config  the configuration the receiver is created from
public record ReceiverEntry<C extends ComponentConfig>(ReceiverFactory<C> factory, C config) {

    /// Validates that neither component is null.
    public ReceiverEntry {
        if (factory == null) {
            throw new IllegalArgumentException("receiver factory must be non-null");
        }
        if (config == null) {
            throw new IllegalArgumentException("receiver config must be non-null");
        }
    }

    /// Creates the receiver from all its routed consumers at once, capturing the factory's config
    /// type internally.
    ///
    /// @param ctx       the create context (identity and base context)
    /// @param consumers the routed consumers, one per served signal
    /// @return the receiver
    public Receiver create(CreateContext ctx, SignalConsumers consumers) {
        return factory.create(ctx, config, consumers);
    }

    /// The kind this entry produces, always [ComponentKind#RECEIVER].
    ///
    /// @return the component kind
    public ComponentKind kind() {
        return factory.kind();
    }

    /// The receiver type name, forming the `type` half of the component id.
    ///
    /// @return the type name
    public String type() {
        return factory.type();
    }
}
