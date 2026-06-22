package dev.nthings.otlp4j.spi;

/// Service-provider interface for creating [OtlpClient]s.
///
/// The `otlp4j-api` module `uses` this service; the transport module `provides` it.
/// `OtlpGrpcExporter` locates an implementation via `ServiceLoader`.
public interface OtlpClientProvider {

    /// Creates a client honouring `config`.
    OtlpClient create(ClientTransportConfig config);
}
