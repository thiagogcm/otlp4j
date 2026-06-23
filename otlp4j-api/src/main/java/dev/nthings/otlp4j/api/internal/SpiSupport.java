package dev.nthings.otlp4j.api.internal;

import dev.nthings.otlp4j.spi.Protocol;
import dev.nthings.otlp4j.spi.TransportProvider;
import java.util.ServiceLoader;

/// Locates transport service providers for the API module via `ServiceLoader`.
///
/// Internal to the API module; not part of the public surface.
public final class SpiSupport {

    private SpiSupport() {}

    /// Returns the provider of `service` that implements `protocol`. Providers are selected by
    /// [Protocol] rather than by `ServiceLoader` order, so the gRPC and HTTP transports can sit on
    /// the path together. Throws when no provider implements the protocol, and (rather than let
    /// order pick silently) when more than one does, naming the candidates.
    public static <T extends TransportProvider> T provider(Class<T> service, Protocol protocol) {
        var all = ServiceLoader.load(service).stream().map(ServiceLoader.Provider::get).toList();
        var matches = all.stream().filter(p -> p.protocol() == protocol).toList();
        if (matches.isEmpty()) {
            var available = all.stream().map(p -> p.protocol().name()).distinct().toList();
            throw new IllegalStateException(
                    "No " + service.getSimpleName() + " for protocol " + protocol
                            + " found on the module/class path. Add the otlp4j-transport module to "
                            + "provide the OTLP transports"
                            + (available.isEmpty() ? "." : "; providers present implement: " + available + "."));
        }
        if (matches.size() > 1) {
            var names = matches.stream().map(p -> p.getClass().getName()).toList();
            throw new IllegalStateException(
                    "Multiple " + service.getSimpleName() + " providers for protocol " + protocol
                            + " found on the module/class path; the choice is ambiguous: " + names
                            + ". Keep exactly one provider per protocol on the path.");
        }
        return matches.getFirst();
    }
}
