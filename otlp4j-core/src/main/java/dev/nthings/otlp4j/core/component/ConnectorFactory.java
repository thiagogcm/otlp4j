package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalPair;
import java.util.Set;

/// A factory for connectors. It declares the signal pairs it supports, which the graph validates as
/// edge metadata, and creates one connector bound to a chosen pair, emitting into the receiving
/// pipeline's head consumer.
///
/// @param <C> the typed configuration this factory accepts
public interface ConnectorFactory<C extends ComponentConfig> extends ComponentFactory<C> {

    /// The signal pairs this connector supports, each an input-to-output pairing.
    ///
    /// @return the supported pairs
    Set<SignalPair<?, ?>> supportedPairs();

    /// Creates one connector for `pair` that emits into `next`.
    ///
    /// @param ctx    the create context (identity and base context)
    /// @param config the connector configuration
    /// @param pair   the signal pair this connector bridges
    /// @param next   the receiving pipeline head this connector emits into
    /// @return the connector
    Connector create(CreateContext ctx, C config, SignalPair<?, ?> pair, SignalConsumer<?> next);
}
