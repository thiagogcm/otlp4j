package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.spi.OtlpClientProvider;

/// [OtlpClientProvider] backed by gRPC — the service this module provides for `OtlpGrpcExporter`
/// to discover via `ServiceLoader`.
public final class GrpcOtlpClientProvider implements OtlpClientProvider {

    /// Public no-arg constructor required by `ServiceLoader`.
    public GrpcOtlpClientProvider() {}

    @Override
    public OtlpClient create(ClientTransportConfig config) {
        return new GrpcOtlpClient(config);
    }
}
