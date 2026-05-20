package dev.nthings.otlp4j.api.internal;

import java.util.ServiceLoader;
import java.util.function.Function;

/// Locates and memoises transport service providers for the API module.
///
/// Internal to `dev.nthings.otlp4j.api`; not part of the public surface. Providers expose a
/// `name()` method so multiple transports can coexist on the runtime path; users select the
/// one they want by name. The first provider is returned when no name is given, preserving
/// the v0 single-transport behaviour.
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

    /// Returns the named provider for `service`. Throws when none matches.
    public static <T> T provider(Class<T> service, String name, Function<T, String> nameOf) {
        for (var p : ServiceLoader.load(service)) {
            if (nameOf.apply(p).equals(name)) {
                return p;
            }
        }
        throw new IllegalStateException(
                "No " + service.getSimpleName() + " named '" + name + "' found on the module/class path.");
    }
}
