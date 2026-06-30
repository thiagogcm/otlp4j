/// Transport SPI: the shared `ClientExporter` / `ServerReceiver` composition primitives the bundled
/// OTLP transports build on (each wraps an `OtlpClient` / `OtlpServer`), plus the package-private
/// receiver dispatch and tap plumbing. Most callers use the `OtlpGrpc*` / `OtlpHttp*` entry points
/// instead.
module dev.nthings.otlp4j.transport.spi {
    requires dev.nthings.otlp4j.model;
    // The shared impls expose api types in their signatures, so readers must read api transitively.
    requires transitive dev.nthings.otlp4j.api;
    requires org.slf4j;

    exports dev.nthings.otlp4j.transport.spi;
}
