package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.pipeline.TelemetryConsumer;

/// Terminal [TelemetryConsumer] with a lifecycle.
///
/// Implement this to send telemetry to a custom destination; see [OtlpGrpcExporter] for OTLP/gRPC.
public interface Exporter extends TelemetryConsumer, AutoCloseable {

    /// Releases the exporter's resources. The default does nothing.
    @Override
    default void close() {}
}
