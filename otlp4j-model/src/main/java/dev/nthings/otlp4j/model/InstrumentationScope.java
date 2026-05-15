package dev.nthings.otlp4j.model;

/// The instrumentation scope (library name/version) that produced a batch of telemetry.
/// Mirrors `opentelemetry.proto.common.v1.InstrumentationScope`.
public record InstrumentationScope(
        String name, String version, Attributes attributes, int droppedAttributesCount) {

    public static final InstrumentationScope EMPTY =
            new InstrumentationScope("", "", Attributes.empty(), 0);
}
