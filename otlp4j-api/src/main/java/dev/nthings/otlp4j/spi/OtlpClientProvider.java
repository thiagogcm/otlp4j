package dev.nthings.otlp4j.spi;

/// Service-provider interface for creating [OtlpClient]s.
///
/// The `otlp4j-api` module `uses` this service; the transport module `provides` it. The high-level
/// exporters locate an implementation via `ServiceLoader`, selecting by [#protocol()] so the gRPC
/// and HTTP clients can coexist on the path.
public interface OtlpClientProvider extends TransportProvider {

    /// Creates a client honouring `config`.
    OtlpClient create(ClientTransportConfig config);
}
