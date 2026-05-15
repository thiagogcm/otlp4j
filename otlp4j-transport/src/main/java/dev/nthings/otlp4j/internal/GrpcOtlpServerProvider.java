package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.spi.OtlpServerProvider;

/// [OtlpServerProvider] backed by gRPC — the service this module `provides` for the
/// API's `OtlpReceiver` to discover via [java.util.ServiceLoader].
public final class GrpcOtlpServerProvider implements OtlpServerProvider {

    /// Public no-arg constructor required by [java.util.ServiceLoader].
    public GrpcOtlpServerProvider() {}

    @Override
    public OtlpServer create(TelemetryConsumer consumer) {
        return new GrpcOtlpServer(consumer);
    }
}
