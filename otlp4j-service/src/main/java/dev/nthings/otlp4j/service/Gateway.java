package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.service.config.GatewayConfig;

/// The entry point to the graph runtime. [#builder] returns the one fluent builder that registers
/// components, returns typed handles, and compiles to a
/// [dev.nthings.otlp4j.service.config.GatewayConfig]; [#compile] is the declarative alternative for a
/// caller that constructs a [GatewayConfig] directly. Both paths pass through the same materialization.
public final class Gateway {

    private Gateway() {}

    /// A fresh fluent builder for a gateway.
    ///
    /// @return the builder
    public static GatewayBuilder builder() {
        return new GatewayBuilder();
    }

    /// Validates and materializes a directly-built [GatewayConfig] into an unstarted runtime with a
    /// default component context, the declarative construction path.
    ///
    /// @param config the gateway configuration
    /// @return the unstarted runtime
    public static GatewayRuntime compile(GatewayConfig config) {
        return new GatewayCompiler(config).build();
    }

    /// Validates and materializes a directly-built [GatewayConfig] into an unstarted runtime with the
    /// given base component context.
    ///
    /// @param config the gateway configuration
    /// @param base   the base component context every component is created and started with
    /// @return the unstarted runtime
    public static GatewayRuntime compile(GatewayConfig config, ComponentContext base) {
        return new GatewayCompiler(config).build(base);
    }
}
