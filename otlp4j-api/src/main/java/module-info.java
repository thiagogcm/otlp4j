/// The high-level, user-facing otlp4j API: typed model, pipeline DSL, exporter/receiver abstractions,
/// configuration, and the transport SPI contracts — no generated proto code or gRPC. The protocol
/// modules (`otlp4j-transport-grpc`/`-http`) supply the concrete exporters/receivers.
module dev.nthings.otlp4j.api {
    // Re-export the model so api signatures expose model types without an explicit `requires`.
    requires transitive dev.nthings.otlp4j.model;
    requires transitive org.jspecify;

    // Logging facade; not transitive — consumers bring their own backend.
    requires org.slf4j;

    exports dev.nthings.otlp4j.core;
    exports dev.nthings.otlp4j.pipeline;
    exports dev.nthings.otlp4j.receiver;
    exports dev.nthings.otlp4j.exporter;
    exports dev.nthings.otlp4j.processor;
    exports dev.nthings.otlp4j.connector;
    exports dev.nthings.otlp4j.config;
    exports dev.nthings.otlp4j.spi;
}
