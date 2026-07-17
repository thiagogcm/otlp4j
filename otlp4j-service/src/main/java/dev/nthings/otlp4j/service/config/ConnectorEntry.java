package dev.nthings.otlp4j.service.config;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.Connector;
import dev.nthings.otlp4j.core.component.ConnectorFactory;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalPair;
import java.util.Set;

/// A connector's declarative entry in a [GatewayConfig]: the factory paired with its typed
/// configuration. The entry hides the factory's config type parameter behind [#create] and exposes
/// the factory's declared [#supportedPairs] so the graph validates the bridged signal pairing as
/// edge metadata before the connector is created.
///
/// @param <C>     the typed configuration this connector's factory accepts
/// @param factory the factory that produces the connector
/// @param config  the configuration the connector is created from
public record ConnectorEntry<C extends ComponentConfig>(ConnectorFactory<C> factory, C config) {

    /// Validates that neither component is null.
    public ConnectorEntry {
        if (factory == null) {
            throw new IllegalArgumentException("connector factory must be non-null");
        }
        if (config == null) {
            throw new IllegalArgumentException("connector config must be non-null");
        }
    }

    /// The signal pairs this connector supports, each an input-to-output pairing.
    ///
    /// @return the supported pairs
    public Set<SignalPair<?, ?>> supportedPairs() {
        return factory.supportedPairs();
    }

    /// Creates the connector for `pair`, emitting into `next`, capturing the factory's config type
    /// internally.
    ///
    /// @param ctx  the create context (identity and base context)
    /// @param pair the signal pair this connector bridges
    /// @param next the receiving pipeline head this connector emits into
    /// @return the connector
    public Connector create(CreateContext ctx, SignalPair<?, ?> pair, SignalConsumer<?> next) {
        return factory.create(ctx, config, pair, next);
    }
}
