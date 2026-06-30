/// Internal transport SPI: the abstract exporter/receiver bases the bundled OTLP transports
/// subclass, plus the package-private receiver dispatch and tap plumbing. Not part of the public
/// otlp4j API — read by the gRPC and HTTP transport modules.
module dev.nthings.otlp4j.transport.spi {
    requires dev.nthings.otlp4j.model;
    // Bases expose api types in their signatures, so readers must read api transitively.
    requires transitive dev.nthings.otlp4j.api;
    requires org.slf4j;

    exports dev.nthings.otlp4j.transport.spi;
}
