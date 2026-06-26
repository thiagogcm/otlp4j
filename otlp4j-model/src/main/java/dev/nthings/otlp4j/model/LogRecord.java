package dev.nthings.otlp4j.model;

import java.util.Objects;

/// A single log record. Mirrors `opentelemetry.proto.logs.v1.LogRecord`.
///
/// `body` is an arbitrary [AttributeValue]. Trace and span identifiers are lowercase-hex strings
/// when present. Prefer [#builder()] over the positional constructor.
public record LogRecord(
        long epochNanos,
        long observedEpochNanos,
        Severity severity,
        String severityText,
        AttributeValue body,
        Attributes attributes,
        int droppedAttributesCount,
        long flags,
        String traceId,
        String spanId,
        String eventName) {

    public LogRecord {
        // Validate ids and flags at construction, not later on the async export thread.
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(severityText, "severityText");
        Objects.requireNonNull(body, "body");
        Objects.requireNonNull(attributes, "attributes");
        Objects.requireNonNull(eventName, "eventName");
        traceId = Ids.traceId(traceId);
        spanId = Ids.spanId(spanId);
        flags = Ids.flags(flags);
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Returns a [Builder] pre-populated with this record's fields, for copy-modify transforms.
    public Builder toBuilder() {
        return new Builder()
                .epochNanos(epochNanos)
                .observedEpochNanos(observedEpochNanos)
                .severity(severity)
                .severityText(severityText)
                .body(body)
                .attributes(attributes)
                .droppedAttributesCount(droppedAttributesCount)
                .flags(flags)
                .traceId(traceId)
                .spanId(spanId)
                .eventName(eventName);
    }

    /// Normalized severity level, per the OpenTelemetry log data model.
    public enum Severity implements ProtoEnum {
        UNSPECIFIED(0),
        TRACE(1),
        TRACE2(2),
        TRACE3(3),
        TRACE4(4),
        DEBUG(5),
        DEBUG2(6),
        DEBUG3(7),
        DEBUG4(8),
        INFO(9),
        INFO2(10),
        INFO3(11),
        INFO4(12),
        WARN(13),
        WARN2(14),
        WARN3(15),
        WARN4(16),
        ERROR(17),
        ERROR2(18),
        ERROR3(19),
        ERROR4(20),
        FATAL(21),
        FATAL2(22),
        FATAL3(23),
        FATAL4(24);

        private final int number;

        Severity(int number) {
            this.number = number;
        }

        /// The numeric severity as defined by the protocol (0-24).
        @Override
        public int number() {
            return number;
        }

        // Cached to avoid values()'s per-call array clone on the decode hot path.
        private static final Severity[] VALUES = values();

        /// Resolves a severity from its protocol number, falling back to [#UNSPECIFIED].
        public static Severity fromNumber(int number) {
            return ProtoEnum.fromNumber(VALUES, number, UNSPECIFIED);
        }
    }

    /// Fluent builder for [LogRecord]. Every field defaults to its empty/zero value.
    public static final class Builder {

        private long epochNanos;
        private long observedEpochNanos;
        private Severity severity = Severity.UNSPECIFIED;
        private String severityText = "";
        private AttributeValue body = AttributeValue.empty();
        private Attributes attributes = Attributes.empty();
        private int droppedAttributesCount;
        private long flags;
        private String traceId = "";
        private String spanId = "";
        private String eventName = "";

        private Builder() {}

        public Builder epochNanos(long epochNanos) {
            this.epochNanos = epochNanos;
            return this;
        }

        public Builder observedEpochNanos(long observedEpochNanos) {
            this.observedEpochNanos = observedEpochNanos;
            return this;
        }

        public Builder severity(Severity severity) {
            this.severity = severity;
            return this;
        }

        public Builder severityText(String severityText) {
            this.severityText = severityText;
            return this;
        }

        public Builder body(AttributeValue body) {
            this.body = body;
            return this;
        }

        public Builder body(String body) {
            this.body = AttributeValue.of(body);
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

        public Builder flags(long flags) {
            this.flags = flags;
            return this;
        }

        public Builder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public Builder spanId(String spanId) {
            this.spanId = spanId;
            return this;
        }

        public Builder eventName(String eventName) {
            this.eventName = eventName;
            return this;
        }

        public LogRecord build() {
            return new LogRecord(
                    epochNanos,
                    observedEpochNanos,
                    severity,
                    severityText,
                    body,
                    attributes,
                    droppedAttributesCount,
                    flags,
                    traceId,
                    spanId,
                    eventName);
        }
    }
}
