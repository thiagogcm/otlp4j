package dev.nthings.otlp4j.spi;

import java.time.Duration;

/// Service-provider interface for creating [OtlpClient]s.
///
/// The `otlp4j-api` module `uses` this service; `otlp4j-transport`
/// `provides` it. `OtlpGrpcExporter` locates an implementation via
/// [java.util.ServiceLoader].
public interface OtlpClientProvider {

    /// Creates a client targeting `host:port`, applying `timeout` as the per-export deadline.
    OtlpClient create(String host, int port, Duration timeout);
}
