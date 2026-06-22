package dev.nthings.otlp4j.api.internal;

import java.util.ServiceLoader;

/// Locates transport service providers for the API module via [java.util.ServiceLoader].
///
/// Internal to `dev.nthings.otlp4j.api`; not part of the public surface.
public final class SpiSupport {

    private SpiSupport() {}

    /// Returns the single provider for `service`. Throws when none is present, and (rather than let
    /// `ServiceLoader` order pick silently) also when more than one is, naming the candidates.
    public static <T> T firstProvider(Class<T> service) {
        var providers = ServiceLoader.load(service).stream().toList();
        if (providers.isEmpty()) {
            throw new IllegalStateException(
                    "No " + service.getSimpleName() + " found on the module/class path. "
                            + "Add the otlp4j-transport module to provide the OTLP/gRPC transport.");
        }
        if (providers.size() > 1) {
            var names = providers.stream().map(p -> p.type().getName()).toList();
            throw new IllegalStateException(
                    "Multiple " + service.getSimpleName() + " providers found on the module/class path; "
                            + "provider order is not a configuration mechanism, so the choice is ambiguous: "
                            + names + ". Keep exactly one transport provider on the path.");
        }
        return providers.getFirst().get();
    }
}
