package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.spi.OtlpClientProvider;
import java.time.Duration;

/// [OtlpClientProvider] backed by gRPC — the service this module `provides` for the
/// API's `OtlpGrpcExporter` to discover via [java.util.ServiceLoader].
public final class GrpcOtlpClientProvider implements OtlpClientProvider {

    /// Public no-arg constructor required by [java.util.ServiceLoader].
    public GrpcOtlpClientProvider() {}

    @Override
    public OtlpClient create(String host, int port, Duration timeout) {
        return new GrpcOtlpClient(host, port, timeout);
    }
}
