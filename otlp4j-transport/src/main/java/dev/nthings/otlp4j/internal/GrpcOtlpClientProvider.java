package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.spi.OtlpClientProvider;

/// [OtlpClientProvider] backed by gRPC — the service this module provides for `OtlpGrpcExporter`
/// to discover via [java.util.ServiceLoader].
public final class GrpcOtlpClientProvider implements OtlpClientProvider {

    /// Public no-arg constructor required by [java.util.ServiceLoader].
    public GrpcOtlpClientProvider() {}

    @Override
    public String name() {
        return "grpc";
    }

    @Override
    public OtlpClient create(ClientTransportConfig config) {
        return new GrpcOtlpClient(config);
    }
}
