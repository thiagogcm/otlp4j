package dev.nthings.otlp4j.spi;

/// Common supertype of the transport service-provider interfaces ([OtlpClientProvider] and
/// [OtlpServerProvider]), carrying the [Protocol] discriminator the high-level API uses to pick a
/// provider when several are on the path.
///
/// Provider order is not a configuration mechanism, so selection is by protocol: the API loads every
/// provider via `ServiceLoader` and keeps the one whose [#protocol()] matches the transport the
/// caller asked for.
public interface TransportProvider {

    /// The protocol this provider implements.
    Protocol protocol();
}
