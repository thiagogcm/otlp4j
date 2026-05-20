package dev.nthings.otlp4j.spi;

/// Service-provider interface for creating [OtlpClient]s.
///
/// The `otlp4j-api` module `uses` this service; the transport module `provides` it.
/// `OtlpGrpcExporter` locates an implementation via [java.util.ServiceLoader].
public interface OtlpClientProvider {

    /// A short identifier (e.g., `"grpc"`, `"http/protobuf"`) used to disambiguate when multiple
    /// providers are on the runtime path.
    String name();

    /// Creates a client honouring `config`.
    OtlpClient create(ClientTransportConfig config);
}
