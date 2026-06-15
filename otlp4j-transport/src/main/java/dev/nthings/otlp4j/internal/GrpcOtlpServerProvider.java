package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import dev.nthings.otlp4j.spi.ServerTransportConfig;

/// [OtlpServerProvider] backed by gRPC — the service this module provides for `OtlpGrpcReceiver`
/// to discover via [java.util.ServiceLoader].
public final class GrpcOtlpServerProvider implements OtlpServerProvider {

    /// Public no-arg constructor required by [java.util.ServiceLoader].
    public GrpcOtlpServerProvider() {}

    @Override
    public OtlpServer create(ServerTransportConfig config, Dispatchers dispatchers) {
        return new GrpcOtlpServer(config, dispatchers);
    }
}
