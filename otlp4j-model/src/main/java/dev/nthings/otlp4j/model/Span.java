package dev.nthings.otlp4j.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// A single operation within a trace. Mirrors `opentelemetry.proto.trace.v1.Span`.
///
/// Trace and span identifiers are lowercase-hex strings. Prefer [#builder()] over the positional
/// constructor.
public record Span(
        String traceId,
        String spanId,
        String parentSpanId,
        String traceState,
        long flags,
        String name,
        Kind kind,
        long startEpochNanos,
        long endEpochNanos,
        Attributes attributes,
        int droppedAttributesCount,
        List<Event> events,
        int droppedEventsCount,
        List<Link> links,
        int droppedLinksCount,
        Status status) {

    public Span {
        // Validate ids, flags, and nulls at construction, not later on the async export thread.
        traceId = Ids.traceId(traceId);
        spanId = Ids.spanId(spanId);
        parentSpanId = Ids.parentSpanId(parentSpanId);
        flags = Ids.flags(flags);
        Objects.requireNonNull(traceState, "traceState");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(attributes, "attributes");
        Objects.requireNonNull(status, "status");
        events = List.copyOf(events);
        links = List.copyOf(links);
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Explicit OTLP wire numbers decouple the encoding from declaration order.
    public enum Kind implements ProtoEnum {
        UNSPECIFIED(0),
        INTERNAL(1),
        SERVER(2),
        CLIENT(3),
        PRODUCER(4),
        CONSUMER(5);

        private final int number;

        Kind(int number) {
            this.number = number;
        }

        /// The numeric span kind as defined by the protocol.
        @Override
        public int number() {
            return number;
        }

        // Cached to avoid values()'s per-call array clone on the decode hot path.
        private static final Kind[] VALUES = values();

        /// Resolves a kind from its protocol number, falling back to [#UNSPECIFIED].
        public static Kind fromNumber(int number) {
            return ProtoEnum.fromNumber(VALUES, number, UNSPECIFIED);
        }
    }

    /// A timestamped annotation on a span.
    public record Event(long epochNanos, String name, Attributes attributes, int droppedAttributesCount) {}

    /// A pointer from this span to a related span, possibly in another trace.
    public record Link(
            String traceId,
            String spanId,
            String traceState,
            Attributes attributes,
            int droppedAttributesCount,
            long flags) {

        public Link {
            // Same construction-time guarantees as Span: ids and flags are wire-valid.
            traceId = Ids.traceId(traceId);
            spanId = Ids.spanId(spanId);
            flags = Ids.flags(flags);
        }
    }

    /// The span's final status.
    public record Status(Code code, String message) {

        public static final Status UNSET = new Status(Code.UNSET, "");

        public enum Code implements ProtoEnum {
            UNSET(0),
            OK(1),
            ERROR(2);

            private final int number;

            Code(int number) {
                this.number = number;
            }

            /// The numeric status code as defined by the protocol.
            @Override
            public int number() {
                return number;
            }

            // Cached to avoid values()'s per-call array clone on the decode hot path.
            private static final Code[] VALUES = values();

            /// Resolves a status code from its protocol number, falling back to [#UNSET].
            public static Code fromNumber(int number) {
                return ProtoEnum.fromNumber(VALUES, number, UNSET);
            }
        }
    }

    /// Fluent builder for [Span]. Every field defaults to its empty/zero value.
    public static final class Builder {

        private String traceId = "";
        private String spanId = "";
        private String parentSpanId = "";
        private String traceState = "";
        private long flags;
        private String name = "";
        private Kind kind = Kind.UNSPECIFIED;
        private long startEpochNanos;
        private long endEpochNanos;
        private Attributes attributes = Attributes.empty();
        private int droppedAttributesCount;
        private final List<Event> events = new ArrayList<>();
        private int droppedEventsCount;
        private final List<Link> links = new ArrayList<>();
        private int droppedLinksCount;
        private Status status = Status.UNSET;

        private Builder() {}

        public Builder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public Builder spanId(String spanId) {
            this.spanId = spanId;
            return this;
        }

        public Builder parentSpanId(String parentSpanId) {
            this.parentSpanId = parentSpanId;
            return this;
        }

        public Builder traceState(String traceState) {
            this.traceState = traceState;
            return this;
        }

        public Builder flags(long flags) {
            this.flags = flags;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder kind(Kind kind) {
            this.kind = kind;
            return this;
        }

        public Builder startEpochNanos(long startEpochNanos) {
            this.startEpochNanos = startEpochNanos;
            return this;
        }

        public Builder endEpochNanos(long endEpochNanos) {
            this.endEpochNanos = endEpochNanos;
            return this;
        }

        public Builder attributes(Attributes attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder droppedAttributesCount(int droppedAttributesCount) {
            this.droppedAttributesCount = droppedAttributesCount;
            return this;
        }

        public Builder events(List<Event> events) {
            // Null-check before clear() so a bad arg can't half-mutate the builder.
            Objects.requireNonNull(events, "events");
            this.events.clear();
            this.events.addAll(events);
            return this;
        }

        public Builder addEvent(Event event) {
            this.events.add(event);
            return this;
        }

        public Builder droppedEventsCount(int droppedEventsCount) {
            this.droppedEventsCount = droppedEventsCount;
            return this;
        }

        public Builder links(List<Link> links) {
            Objects.requireNonNull(links, "links");
            this.links.clear();
            this.links.addAll(links);
            return this;
        }

        public Builder addLink(Link link) {
            this.links.add(link);
            return this;
        }

        public Builder droppedLinksCount(int droppedLinksCount) {
            this.droppedLinksCount = droppedLinksCount;
            return this;
        }

        public Builder status(Status status) {
            this.status = status;
            return this;
        }

        public Span build() {
            return new Span(
                    traceId,
                    spanId,
                    parentSpanId,
                    traceState,
                    flags,
                    name,
                    kind,
                    startEpochNanos,
                    endEpochNanos,
                    attributes,
                    droppedAttributesCount,
                    events,
                    droppedEventsCount,
                    links,
                    droppedLinksCount,
                    status);
        }
    }
}
