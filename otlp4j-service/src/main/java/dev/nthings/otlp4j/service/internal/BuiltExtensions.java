package dev.nthings.otlp4j.service.internal;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.Extension;
import dev.nthings.otlp4j.core.component.Extensions;
import java.util.Map;
import java.util.Optional;

/// The [Extensions] view over a gateway build's already-constructed extension map. The builder creates
/// every extension before the data path, collects them keyed by id, and wraps the map in this view so
/// each data-path create context can resolve an authenticator or credential extension the configuration
/// references. Resolution is a plain map lookup by the composite-free extension id, because an id names
/// exactly one extension in a [dev.nthings.otlp4j.service.config.GatewayConfig].
public final class BuiltExtensions implements Extensions {

    private final Map<ComponentId, Extension> byId;

    /// Wraps an immutable copy of the built extension map.
    ///
    /// @param byId the extensions built for this gateway, keyed by id
    public BuiltExtensions(Map<ComponentId, Extension> byId) {
        this.byId = Map.copyOf(byId);
    }

    @Override
    public Optional<Extension> get(ComponentId id) {
        return Optional.ofNullable(byId.get(id));
    }
}
