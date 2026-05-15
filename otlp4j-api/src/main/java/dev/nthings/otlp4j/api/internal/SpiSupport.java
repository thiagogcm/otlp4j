package dev.nthings.otlp4j.api.internal;

import java.util.ServiceLoader;

/// Locates and memoizes transport [service providers][ServiceLoader] for the API module.
///
/// Internal to `dev.nthings.otlp4j.api`; not part of the public surface.
public final class SpiSupport {

    private static final ClassValue<Object> PROVIDERS = new ClassValue<>() {
        @Override
        protected Object computeValue(Class<?> service) {
            return ServiceLoader.load(service)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "No " + service.getSimpleName() + " found on the module/class path. "
                                    + "Add the otlp4j-transport module to provide the OTLP/gRPC transport."));
        }
    };

    private SpiSupport() {}

    /// Returns the registered provider for `service`, scanning once and caching the result.
    @SuppressWarnings("unchecked")
    public static <T> T provider(Class<T> service) {
        return (T) PROVIDERS.get(service);
    }
}
