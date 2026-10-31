package dev.nthings.otlp4j.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/// A set of samples of one type collected over a period. Mirrors the `Profile` proto message with
/// every dictionary reference resolved to its value: a [Sample] holds its [Stack], a [Location] its
/// [Mapping] and [Line]s, and a [Line] its [Function]. A profile is therefore self-contained, and
/// transforming, moving, or merging one cannot leave a dangling reference.
///
/// Experimental. Profiles are still `v1development`; this type's shape may change or be removed in a
/// future minor release without notice.
///
/// The profile id is a lowercase-hex string and is empty for an id-less profile. [#durationNanos],
/// [Location#address], and the [Mapping] addresses are unsigned 64-bit values on the wire, carried
/// bit-for-bit in a `long`. `attributeUnits` maps an attribute key to its UCUM unit and holds only the
/// attributes that have one, so a unit whose attribute is gone is dropped at construction. Prefer
/// [#builder()] over the positional constructor.
public record Profile(
        String profileId,
        long epochNanos,
        long durationNanos,
        ValueType sampleType,
        ValueType periodType,
        long period,
        List<Sample> samples,
        Attributes attributes,
        Map<String, String> attributeUnits,
        int droppedAttributesCount,
        String originalPayloadFormat,
        byte[] originalPayload) {

    private static final int MAX_SHARED_UNITS = 64;

    public Profile {
        // Validate at construction, not on the export thread.
        profileId = Ids.profileId(profileId);
        Objects.requireNonNull(sampleType, "sampleType");
        Objects.requireNonNull(periodType, "periodType");
        samples = List.copyOf(samples);
        attributeUnits = units(attributes, attributeUnits);
        Objects.requireNonNull(originalPayloadFormat, "originalPayloadFormat");
        originalPayload = originalPayload.clone();
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Returns a pre-populated [Builder] for copy-modify transforms.
    public Builder toBuilder() {
        return new Builder()
                .profileId(profileId)
                .epochNanos(epochNanos)
                .durationNanos(durationNanos)
                .sampleType(sampleType)
                .periodType(periodType)
                .period(period)
                .samples(samples)
                .attributes(attributes)
                .attributeUnits(attributeUnits)
                .droppedAttributesCount(droppedAttributesCount)
                .originalPayloadFormat(originalPayloadFormat)
                .originalPayload(originalPayload);
    }

    /// The source-format bytes this profile was converted from, or an empty array when none are
    /// carried. The returned array is a defensive clone.
    @Override
    public byte[] originalPayload() {
        return originalPayload.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Profile other
                && profileId.equals(other.profileId)
                && epochNanos == other.epochNanos
                && durationNanos == other.durationNanos
                && sampleType.equals(other.sampleType)
                && periodType.equals(other.periodType)
                && period == other.period
                && samples.equals(other.samples)
                && attributes.equals(other.attributes)
                && attributeUnits.equals(other.attributeUnits)
                && droppedAttributesCount == other.droppedAttributesCount
                && originalPayloadFormat.equals(other.originalPayloadFormat)
                && Arrays.equals(originalPayload, other.originalPayload);
    }

    @Override
    public int hashCode() {
        var result = profileId.hashCode();
        result = 31 * result + Long.hashCode(epochNanos);
        result = 31 * result + Long.hashCode(durationNanos);
        result = 31 * result + sampleType.hashCode();
        result = 31 * result + periodType.hashCode();
        result = 31 * result + Long.hashCode(period);
        result = 31 * result + samples.hashCode();
        result = 31 * result + attributes.hashCode();
        result = 31 * result + attributeUnits.hashCode();
        result = 31 * result + droppedAttributesCount;
        result = 31 * result + originalPayloadFormat.hashCode();
        result = 31 * result + Arrays.hashCode(originalPayload);
        return result;
    }

    @Override
    public String toString() {
        return "Profile[profileId=" + profileId
                + ", epochNanos=" + epochNanos
                + ", durationNanos=" + durationNanos
                + ", sampleType=" + sampleType
                + ", periodType=" + periodType
                + ", period=" + period
                + ", samples=" + samples.size()
                + ", attributes=" + attributes
                + ", attributeUnits=" + attributeUnits
                + ", droppedAttributesCount=" + droppedAttributesCount
                + ", originalPayloadFormat=" + originalPayloadFormat
                + ", originalPayload=" + originalPayload.length + " bytes]";
    }

    /// Returns `units` as an immutable map that records keep without copying it, so that the samples of
    /// a large profile can share one. `Map.copyOf` makes such a map, but it has no defense against keys
    /// that collide and a sender chooses these, so it is used only where collisions cannot cost much.
    ///
    /// @param units attribute keys mapped to their units
    /// @return an immutable copy
    public static Map<String, String> immutableUnits(Map<String, String> units) {
        return units.size() <= MAX_SHARED_UNITS
                ? Map.copyOf(units)
                : Collections.unmodifiableMap(new HashMap<>(units));
    }

    /// Leaves out any unit that names no attribute or says nothing: a transform that removes an
    /// attribute should not have to know whether it had a unit.
    private static Map<String, String> units(Attributes attributes, Map<String, String> units) {
        Objects.requireNonNull(attributes, "attributes");
        for (var unit : units.entrySet()) {
            if (unit.getValue().isEmpty() || !attributes.contains(unit.getKey())) {
                var kept = new HashMap<>(units);
                kept.values().removeIf(String::isEmpty);
                kept.keySet().retainAll(attributes.keys());
                return immutableUnits(kept);
            }
        }
        return immutableUnits(units);
    }

    /// The type and unit of a measured value, such as `cpu` in `nanoseconds`.
    public record ValueType(String type, String unit) {

        /// A value type with neither part known.
        public static final ValueType EMPTY = new ValueType("", "");

        public ValueType {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(unit, "unit");
        }
    }

    /// A program context and the observations recorded in it. Mirrors the `Sample` proto message.
    ///
    /// `values` holds the measurements, typed by [Profile#sampleType], and `timestamps` the Unix epoch
    /// nanoseconds of the observations. OTLP requires at least one to be populated and, when both are,
    /// the same size with elements at the same index describing the same event; a consumer counts each
    /// timestamp of a sample without values as one. A builder from [#builder()] enforces that shape on
    /// what this process produces. The record does not, and neither does a builder from [#toBuilder()],
    /// so that a sample received from a less careful sender is carried as it arrived, through a
    /// transform too, rather than failing its batch.
    public record Sample(
            Stack stack,
            Attributes attributes,
            Map<String, String> attributeUnits,
            Link link,
            List<Long> values,
            List<Long> timestamps) {

        public Sample {
            Objects.requireNonNull(stack, "stack");
            attributeUnits = units(attributes, attributeUnits);
            Objects.requireNonNull(link, "link");
            values = List.copyOf(values);
            timestamps = List.copyOf(timestamps);
        }

        public static Builder builder() {
            return new Builder(true);
        }

        /// Returns a pre-populated [Builder] for copy-modify transforms.
        public Builder toBuilder() {
            return new Builder(false)
                    .stack(stack)
                    .attributes(attributes)
                    .attributeUnits(attributeUnits)
                    .link(link)
                    .values(values)
                    .timestamps(timestamps);
        }

        /// Fluent builder for [Sample]. Fields default to empty.
        public static final class Builder {

            private final boolean enforceShape;
            private Stack stack = Stack.EMPTY;
            private Attributes attributes = Attributes.empty();
            private Map<String, String> attributeUnits = Map.of();
            private Link link = Link.NONE;
            private final List<Long> values = new ArrayList<>();
            private final List<Long> timestamps = new ArrayList<>();

            private Builder(boolean enforceShape) {
                this.enforceShape = enforceShape;
            }

            public Builder stack(Stack stack) {
                this.stack = stack;
                return this;
            }

            public Builder attributes(Attributes attributes) {
                this.attributes = attributes;
                return this;
            }

            public Builder attributeUnits(Map<String, String> attributeUnits) {
                this.attributeUnits = attributeUnits;
                return this;
            }

            public Builder link(Link link) {
                this.link = link;
                return this;
            }

            public Builder values(List<Long> values) {
                Objects.requireNonNull(values, "values");
                this.values.clear();
                this.values.addAll(values);
                return this;
            }

            public Builder addValue(long value) {
                this.values.add(value);
                return this;
            }

            public Builder timestamps(List<Long> timestamps) {
                Objects.requireNonNull(timestamps, "timestamps");
                this.timestamps.clear();
                this.timestamps.addAll(timestamps);
                return this;
            }

            public Builder addTimestamp(long epochNanos) {
                this.timestamps.add(epochNanos);
                return this;
            }

            public Sample build() {
                if (enforceShape && values.isEmpty() && timestamps.isEmpty()) {
                    throw new IllegalArgumentException("a sample must have values or timestamps");
                }
                if (enforceShape && !values.isEmpty() && !timestamps.isEmpty() && values.size() != timestamps.size()) {
                    throw new IllegalArgumentException(
                            "values.size() (" + values.size() + ") must equal timestamps.size() ("
                                    + timestamps.size() + ") when both are populated");
                }
                return new Sample(stack, attributes, attributeUnits, link, values, timestamps);
            }
        }
    }

    /// A stack trace, listed from the leaf frame outward.
    public record Stack(List<Location> locations) {

        /// A stack without frames, for a sample with no program context.
        public static final Stack EMPTY = new Stack(List.of());

        public Stack {
            locations = List.copyOf(locations);
        }
    }

    /// One frame of a [Stack]. Mirrors the `Location` proto message.
    ///
    /// `lines` lists inlined functions first, so the last entry is the caller they were inlined into.
    public record Location(
            Mapping mapping,
            long address,
            List<Line> lines,
            Attributes attributes,
            Map<String, String> attributeUnits) {

        /// A location with nothing known about it.
        public static final Location EMPTY = builder().build();

        public Location {
            Objects.requireNonNull(mapping, "mapping");
            lines = List.copyOf(lines);
            attributeUnits = units(attributes, attributeUnits);
        }

        public static Builder builder() {
            return new Builder();
        }

        /// Returns a pre-populated [Builder] for copy-modify transforms.
        public Builder toBuilder() {
            return new Builder()
                    .mapping(mapping)
                    .address(address)
                    .lines(lines)
                    .attributes(attributes)
                    .attributeUnits(attributeUnits);
        }

        /// Fluent builder for [Location]. Every field defaults to its empty/zero value.
        public static final class Builder {

            private Mapping mapping = Mapping.EMPTY;
            private long address;
            private final List<Line> lines = new ArrayList<>();
            private Attributes attributes = Attributes.empty();
            private Map<String, String> attributeUnits = Map.of();

            private Builder() {}

            public Builder mapping(Mapping mapping) {
                this.mapping = mapping;
                return this;
            }

            public Builder address(long address) {
                this.address = address;
                return this;
            }

            public Builder lines(List<Line> lines) {
                Objects.requireNonNull(lines, "lines");
                this.lines.clear();
                this.lines.addAll(lines);
                return this;
            }

            public Builder addLine(Line line) {
                this.lines.add(line);
                return this;
            }

            public Builder attributes(Attributes attributes) {
                this.attributes = attributes;
                return this;
            }

            public Builder attributeUnits(Map<String, String> attributeUnits) {
                this.attributeUnits = attributeUnits;
                return this;
            }

            public Location build() {
                return new Location(mapping, address, lines, attributes, attributeUnits);
            }
        }
    }

    /// A position in source code. `line` and `column` are 1-based, and 0 means unset.
    public record Line(Function function, long line, long column) {

        public Line {
            Objects.requireNonNull(function, "function");
        }
    }

    /// A function: its readable name, the name the system knows it by (such as a mangled name), its
    /// source file, and its 1-based starting line, where 0 means unset.
    public record Function(String name, String systemName, String filename, long startLine) {

        /// A function with nothing known about it.
        public static final Function EMPTY = new Function("", "", "", 0L);

        public Function {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(systemName, "systemName");
            Objects.requireNonNull(filename, "filename");
        }
    }

    /// The binary mapped into an address range. Mirrors the `Mapping` proto message.
    public record Mapping(
            long memoryStart,
            long memoryLimit,
            long fileOffset,
            String filename,
            Attributes attributes,
            Map<String, String> attributeUnits) {

        /// A mapping with nothing known about it, for a location without one.
        public static final Mapping EMPTY = builder().build();

        public Mapping {
            Objects.requireNonNull(filename, "filename");
            attributeUnits = units(attributes, attributeUnits);
        }

        public static Builder builder() {
            return new Builder();
        }

        /// Returns a pre-populated [Builder] for copy-modify transforms.
        public Builder toBuilder() {
            return new Builder()
                    .memoryStart(memoryStart)
                    .memoryLimit(memoryLimit)
                    .fileOffset(fileOffset)
                    .filename(filename)
                    .attributes(attributes)
                    .attributeUnits(attributeUnits);
        }

        /// Fluent builder for [Mapping]. Every field defaults to its empty/zero value.
        public static final class Builder {

            private long memoryStart;
            private long memoryLimit;
            private long fileOffset;
            private String filename = "";
            private Attributes attributes = Attributes.empty();
            private Map<String, String> attributeUnits = Map.of();

            private Builder() {}

            public Builder memoryStart(long memoryStart) {
                this.memoryStart = memoryStart;
                return this;
            }

            public Builder memoryLimit(long memoryLimit) {
                this.memoryLimit = memoryLimit;
                return this;
            }

            public Builder fileOffset(long fileOffset) {
                this.fileOffset = fileOffset;
                return this;
            }

            public Builder filename(String filename) {
                this.filename = filename;
                return this;
            }

            public Builder attributes(Attributes attributes) {
                this.attributes = attributes;
                return this;
            }

            public Builder attributeUnits(Map<String, String> attributeUnits) {
                this.attributeUnits = attributeUnits;
                return this;
            }

            public Mapping build() {
                return new Mapping(memoryStart, memoryLimit, fileOffset, filename, attributes, attributeUnits);
            }
        }
    }

    /// A pointer from a [Sample] to the span it was recorded in. Identifiers are lowercase-hex strings.
    public record Link(String traceId, String spanId) {

        /// The absence of a link.
        public static final Link NONE = new Link("", "");

        public Link {
            traceId = Ids.traceId(traceId);
            spanId = Ids.spanId(spanId);
        }
    }

    /// Fluent builder for [Profile]. Every field defaults to its empty/zero value.
    public static final class Builder {

        private String profileId = "";
        private long epochNanos;
        private long durationNanos;
        private ValueType sampleType = ValueType.EMPTY;
        private ValueType periodType = ValueType.EMPTY;
        private long period;
        private final List<Sample> samples = new ArrayList<>();
        private Attributes attributes = Attributes.empty();
        private Map<String, String> attributeUnits = Map.of();
        private int droppedAttributesCount;
        private String originalPayloadFormat = "";
        private byte[] originalPayload = new byte[0];

        private Builder() {}

        public Builder profileId(String profileId) {
            this.profileId = profileId;
            return this;
        }

        public Builder epochNanos(long epochNanos) {
            this.epochNanos = epochNanos;
            return this;
        }

        public Builder durationNanos(long durationNanos) {
            this.durationNanos = durationNanos;
            return this;
        }

        public Builder sampleType(ValueType sampleType) {
            this.sampleType = sampleType;
            return this;
        }

        public Builder periodType(ValueType periodType) {
            this.periodType = periodType;
            return this;
        }

        public Builder period(long period) {
            this.period = period;
            return this;
        }

        public Builder samples(List<Sample> samples) {
            Objects.requireNonNull(samples, "samples");
            this.samples.clear();
            this.samples.addAll(samples);
            return this;
        }

        public Builder addSample(Sample sample) {
            this.samples.add(sample);
            return this;
        }

        public Builder attributes(Attributes attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder attributeUnits(Map<String, String> attributeUnits) {
            this.attributeUnits = attributeUnits;
            return this;
        }

        public Builder droppedAttributesCount(int droppedAttributesCount) {
            this.droppedAttributesCount = droppedAttributesCount;
            return this;
        }

        public Builder originalPayloadFormat(String originalPayloadFormat) {
            this.originalPayloadFormat = originalPayloadFormat;
            return this;
        }

        public Builder originalPayload(byte[] originalPayload) {
            this.originalPayload = originalPayload;
            return this;
        }

        public Profile build() {
            return new Profile(
                    profileId,
                    epochNanos,
                    durationNanos,
                    sampleType,
                    periodType,
                    period,
                    samples,
                    attributes,
                    attributeUnits,
                    droppedAttributesCount,
                    originalPayloadFormat,
                    originalPayload);
        }
    }
}
