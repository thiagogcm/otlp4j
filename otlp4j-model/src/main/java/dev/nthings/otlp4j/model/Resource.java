package dev.nthings.otlp4j.model;

/// The entity producing telemetry (e.g. a service, host, or container), described by its
/// [Attributes]. Mirrors `opentelemetry.proto.resource.v1.Resource`.
public record Resource(Attributes attributes, int droppedAttributesCount) {

    public static final Resource EMPTY = new Resource(Attributes.empty(), 0);
}
