package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.pipeline.TelemetryConsumer;

/// Service-provider interface for creating [OtlpServer]s.
///
/// The `otlp4j-api` module `uses` this service; `otlp4j-transport`
/// `provides` it. `OtlpReceiver` locates an implementation via [java.util.ServiceLoader]
/// — so the API module never references the transport or the generated proto code at compile time.
public interface OtlpServerProvider {

    /// Creates a server that delivers every decoded OTLP export to `consumer`.
    OtlpServer create(TelemetryConsumer consumer);
}
