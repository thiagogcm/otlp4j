/// Shared transport layer: the `ClientExporter` / `ServerReceiver` composition primitives
/// that the OTLP transport builds on, plus package-private dispatch and tap plumbing.
/// Most callers use the `OtlpArmeria*` entry points instead.
@SuppressWarnings("module")
module dev.nthings.otlp4j.transport.common {
    requires dev.nthings.otlp4j.model;
    // The codec payload facade (Payloads) and the payload views the exporter facet and the
    // convenience export build. Proto stays off this module's surface because Payloads and the
    // payload types have proto-free signatures.
    requires dev.nthings.otlp4j.codec;
    // The relocated config records expose the neutral core policies (ClientConfig.retry() is a
    // core RetryPolicy), and the component adapters and the receiver and exporter component
    // configs name core component, consumer, result, and signal types directly, so read core
    // transitively.
    requires transitive dev.nthings.otlp4j.core;
    requires org.slf4j;

    exports dev.nthings.otlp4j.transport.common;
    // Relocated from otlp4j-api (which relinquished these packages) keeping their names, so callers
    // that imported them compile unchanged.
    exports dev.nthings.otlp4j.config;
    exports dev.nthings.otlp4j.spi;
}
