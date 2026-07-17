package dev.nthings.otlp4j.service.config;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Extension;
import dev.nthings.otlp4j.core.component.ExtensionFactory;

/// An extension's declarative entry in a [GatewayConfig]: the factory paired with its typed
/// configuration. The entry hides the factory's config type parameter behind [#create]. An extension
/// has no telemetry data path, so it needs no signal or downstream consumer at creation.
///
/// @param <C>     the typed configuration this extension's factory accepts
/// @param factory the factory that produces the extension
/// @param config  the configuration the extension is created from
public record ExtensionEntry<C extends ComponentConfig>(ExtensionFactory<C> factory, C config) {

    /// Validates that neither component is null.
    public ExtensionEntry {
        if (factory == null) {
            throw new IllegalArgumentException("extension factory must be non-null");
        }
        if (config == null) {
            throw new IllegalArgumentException("extension config must be non-null");
        }
    }

    /// Creates the extension, capturing the factory's config type internally.
    ///
    /// @param ctx the create context (identity and base context)
    /// @return the extension
    public Extension create(CreateContext ctx) {
        return factory.create(ctx, config);
    }
}
