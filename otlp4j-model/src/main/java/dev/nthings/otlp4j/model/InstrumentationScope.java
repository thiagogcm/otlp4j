package dev.nthings.otlp4j.model;

import java.util.Objects;

/// The instrumentation scope (library name/version) that produced a batch of telemetry.
/// Mirrors `opentelemetry.proto.common.v1.InstrumentationScope`.
public record InstrumentationScope(
        String name, String version, Attributes attributes, int droppedAttributesCount) {

    public InstrumentationScope {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(attributes, "attributes");
    }

    public static final InstrumentationScope EMPTY =
            new InstrumentationScope("", "", Attributes.empty(), 0);

    /// A scope identified only by `name`, with no version or attributes.
    public static InstrumentationScope of(String name) {
        return new InstrumentationScope(name, "", Attributes.empty(), 0);
    }

    /// A scope identified by `name` and `version`, with no attributes.
    public static InstrumentationScope of(String name, String version) {
        return new InstrumentationScope(name, version, Attributes.empty(), 0);
    }
}
