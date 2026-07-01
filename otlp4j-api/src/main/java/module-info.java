/// The high-level, user-facing otlp4j API: typed model, pipeline DSL, exporter/receiver abstractions,
/// configuration, and the transport SPI contracts - no generated proto code or gRPC. The protocol
/// modules (otlp4j-transport-grpc/-http) supply the concrete exporters/receivers.
// The qualified export below names transport.spi, a downstream module absent from this module's own
// compile graph; suppress the resulting "module not found" lint so it stays out of failOnWarning.
@SuppressWarnings({"module", "requires-transitive-automatic"})
module dev.nthings.otlp4j.api {
    // Re-export the model so api signatures expose model types without an explicit requires.
    requires transitive dev.nthings.otlp4j.model;
    requires transitive io.github.resilience4j.core;
    requires transitive io.github.resilience4j.retry;
    requires static transitive org.jspecify;

    // Logging facade; not transitive - consumers bring their own backend.
    requires org.slf4j;

    exports dev.nthings.otlp4j.pipeline;
    // Shared-lifecycle wiring the bundled transports' ClientExporter hooks into; not public surface.
    exports dev.nthings.otlp4j.pipeline.internal to dev.nthings.otlp4j.transport.spi;
    exports dev.nthings.otlp4j.receiver;
    exports dev.nthings.otlp4j.processor;
    exports dev.nthings.otlp4j.config;
    exports dev.nthings.otlp4j.spi;
}
