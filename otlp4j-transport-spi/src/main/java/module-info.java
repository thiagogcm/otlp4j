/// Transport SPI: shared [ClientExporter] / [ServerReceiver] composition primitives
/// that bundled OTLP transports build on, plus package-private dispatch and tap plumbing.
/// Most callers use `OtlpGrpc*` / `OtlpHttp*` entry points instead.
module dev.nthings.otlp4j.transport.spi {
    requires dev.nthings.otlp4j.model;
    // Shared impls expose api types in signatures; readers must read api transitively.
    requires transitive dev.nthings.otlp4j.api;
    requires org.slf4j;

    exports dev.nthings.otlp4j.transport.spi;
}
