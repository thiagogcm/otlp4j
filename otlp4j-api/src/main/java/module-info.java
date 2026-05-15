/// The high-level, user-facing otlp4j API.
///
/// This pure module exposes typed model objects and transport SPI contracts, but does not require
/// generated proto code or gRPC. Runtime transports are discovered with [java.util.ServiceLoader].
module dev.nthings.otlp4j.api {
    // The domain model is its own module; re-export it transitively so consumers of this API
    // see the model types in api signatures without an explicit `requires`.
    requires transitive dev.nthings.otlp4j.model;

    // Logging facade; not transitive — consumers bring their own logging stack.
    requires org.slf4j;

    exports dev.nthings.otlp4j.pipeline;
    exports dev.nthings.otlp4j.receiver;
    exports dev.nthings.otlp4j.exporter;
    exports dev.nthings.otlp4j.processor;
    exports dev.nthings.otlp4j.connector;
    exports dev.nthings.otlp4j.spi;

    uses dev.nthings.otlp4j.spi.OtlpServerProvider;
    uses dev.nthings.otlp4j.spi.OtlpClientProvider;
}
