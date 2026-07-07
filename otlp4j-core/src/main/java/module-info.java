/// The public contract surface of the otlp4j gateway: component identity and kind, pipeline
/// identity, signal descriptors, consumers and delivery context, delivery results, the neutral
/// policy vocabulary, the component lifecycle and factory contracts, and the self-observability
/// telemetry SPI. No transport, no third-party wire library, and no resilience library appears on
/// this module's surface.
///
/// The `dev.nthings.otlp4j.model` domain records are re-exported transitively because signal
/// descriptors name the domain batch types in their signatures. The `org.slf4j` facade is required
/// non-transitively for the logging telemetry adapter alone.
module dev.nthings.otlp4j.core {
    requires transitive dev.nthings.otlp4j.model;
    requires static transitive org.jspecify;
    requires org.slf4j;

    exports dev.nthings.otlp4j.core.auth;
    exports dev.nthings.otlp4j.core.component;
    exports dev.nthings.otlp4j.core.config;
    exports dev.nthings.otlp4j.core.consumer;
    exports dev.nthings.otlp4j.core.pipeline;
    exports dev.nthings.otlp4j.core.policy;
    exports dev.nthings.otlp4j.core.result;
    exports dev.nthings.otlp4j.core.signal;
    exports dev.nthings.otlp4j.telemetry;
}
