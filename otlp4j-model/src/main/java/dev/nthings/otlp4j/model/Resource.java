package dev.nthings.otlp4j.model;

import java.util.Objects;

/// The entity producing telemetry (e.g. a service, host, or container), described by its
/// [Attributes]. Mirrors `opentelemetry.proto.resource.v1.Resource`.
public record Resource(Attributes attributes, int droppedAttributesCount) {

    public Resource {
        Objects.requireNonNull(attributes, "attributes");
    }

    public static final Resource EMPTY = new Resource(Attributes.empty(), 0);

    /// A resource carrying `attributes` and no dropped-attribute count.
    public static Resource of(Attributes attributes) {
        return new Resource(attributes, 0);
    }

    /// Returns a copy of this resource with `key` added to (or replacing it in) its attributes.
    public Resource withAttribute(String key, AttributeValue value) {
        return new Resource(attributes.with(key, value), droppedAttributesCount);
    }
}
