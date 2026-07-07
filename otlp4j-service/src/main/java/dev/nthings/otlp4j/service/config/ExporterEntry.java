package dev.nthings.otlp4j.service.config;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// An exporter's declarative entry in a [GatewayConfig]: the factory paired with its typed
/// configuration. The entry hides the factory's config type parameter behind [#create], which
/// captures it internally so the graph builder wires the exporter to its signal without an unchecked
/// cast.
///
/// @param <C>     the typed configuration this exporter's factory accepts
/// @param factory the factory that produces the exporter
/// @param config  the configuration the exporter is created from
public record ExporterEntry<C extends ComponentConfig>(ExporterFactory<C> factory, C config) {

    /// Validates that neither component is null.
    public ExporterEntry {
        if (factory == null) {
            throw new IllegalArgumentException("exporter factory must be non-null");
        }
        if (config == null) {
            throw new IllegalArgumentException("exporter config must be non-null");
        }
    }

    /// Creates the exporter for `signal`, capturing the factory's config type internally.
    ///
    /// @param ctx    the create context (identity and base context)
    /// @param signal the signal this exporter operates on
    /// @param <T>    the payload type
    /// @return the exporter
    public <T extends TelemetryPayload> Exporter<T> create(CreateContext ctx, SignalDescriptor<T> signal) {
        return factory.create(ctx, config, signal);
    }

    /// The kind this entry produces, always [ComponentKind#EXPORTER].
    ///
    /// @return the component kind
    public ComponentKind kind() {
        return factory.kind();
    }

    /// The exporter type name, forming the `type` half of the component id.
    ///
    /// @return the type name
    public String type() {
        return factory.type();
    }
}
