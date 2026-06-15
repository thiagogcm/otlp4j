package dev.nthings.otlp4j.api.internal;

import java.util.ServiceLoader;

/// Locates transport service providers for the API module via [java.util.ServiceLoader].
///
/// Internal to `dev.nthings.otlp4j.api`; not part of the public surface.
public final class SpiSupport {

    private SpiSupport() {}

    /// Returns the first provider for `service`. Throws when none is present.
    public static <T> T firstProvider(Class<T> service) {
        return ServiceLoader.load(service)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No " + service.getSimpleName() + " found on the module/class path. "
                                + "Add the otlp4j-transport module to provide the OTLP/gRPC transport."));
    }
}
