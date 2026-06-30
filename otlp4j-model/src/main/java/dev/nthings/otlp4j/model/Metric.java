package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;

/// A named metric and its time series. Mirrors the [Metric] proto message.
///
/// The `data` field is a non-null sealed [Data] type; the empty [NoData] variant models the
/// wire `DATA_NOT_SET` form (round-tripped faithfully), so [#data()] is never null.
public record Metric(String name, String description, String unit, Data data, Attributes metadata) {

    public Metric {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(unit, "unit");
        Objects.requireNonNull(data, "data (use Metric.noData() for the empty DATA_NOT_SET form)");
        Objects.requireNonNull(metadata, "metadata");
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Returns a pre-populated [Builder] for copy-modify transforms.
    public Builder toBuilder() {
        return new Builder()
                .name(name)
                .description(description)
                .unit(unit)
                .data(data)
                .metadata(metadata);
    }

    /// Whether this metric carries a data payload. `false` mirrors the wire `DATA_NOT_SET` form.
    public boolean hasData() {
        return !(data instanceof NoData);
    }

    /// Returns the data payload, or throws if this metric is the empty `DATA_NOT_SET` form.
    public Data dataOrThrow() {
        if (data instanceof NoData) {
            throw new IllegalStateException("metric '" + name + "' has no data (DATA_NOT_SET)");
        }
        return data;
    }

    /// A [Gauge] payload over the given points. For a `List`, use `new Gauge(points)`.
    public static Gauge gauge(NumberPoint... points) {
        return new Gauge(List.of(points));
    }

    /// A [Sum] payload with the given temporality and monotonicity.
    public static Sum sum(AggregationTemporality temporality, boolean monotonic, NumberPoint... points) {
        return new Sum(List.of(points), temporality, monotonic);
    }

    /// A [Histogram] payload with the given temporality.
    public static Histogram histogram(AggregationTemporality temporality, HistogramPoint... points) {
        return new Histogram(List.of(points), temporality);
    }

    /// An [ExponentialHistogram] payload with the given temporality.
    public static ExponentialHistogram exponentialHistogram(
            AggregationTemporality temporality, ExponentialHistogramPoint... points) {
        return new ExponentialHistogram(List.of(points), temporality);
    }

    /// A [Summary] payload over the given points. For a `List`, use `new Summary(points)`.
    public static Summary summary(SummaryPoint... points) {
        return new Summary(List.of(points));
    }

    /// The empty `DATA_NOT_SET` payload (the shared [NoData] form).
    public static NoData noData() {
        return NoData.INSTANCE;
    }

    /// The aggregation kind of a metric and its data points.
    public sealed interface Data
            permits Gauge, Sum, Histogram, ExponentialHistogram, Summary, NoData {}

    public record Gauge(List<NumberPoint> points) implements Data {
        public Gauge {
            points = List.copyOf(points);
        }
    }

    public record Sum(List<NumberPoint> points, AggregationTemporality temporality, boolean monotonic)
            implements Data {
        public Sum {
            Objects.requireNonNull(temporality, "temporality");
            points = List.copyOf(points);
        }
    }

    public record Histogram(List<HistogramPoint> points, AggregationTemporality temporality)
            implements Data {
        public Histogram {
            Objects.requireNonNull(temporality, "temporality");
            points = List.copyOf(points);
        }
    }

    public record ExponentialHistogram(
            List<ExponentialHistogramPoint> points, AggregationTemporality temporality)
            implements Data {
        public ExponentialHistogram {
            Objects.requireNonNull(temporality, "temporality");
            points = List.copyOf(points);
        }
    }

    public record Summary(List<SummaryPoint> points) implements Data {
        public Summary {
            points = List.copyOf(points);
        }
    }

    /// The empty payload, mirroring the wire `DATA_NOT_SET` form.
    public record NoData() implements Data {
        public static final NoData INSTANCE = new NoData();
    }

    /// How a metric aggregator reports values relative to time.
    public enum AggregationTemporality implements ProtoEnum {
        UNSPECIFIED(0),
        DELTA(1),
        CUMULATIVE(2);

        private final int number;

        AggregationTemporality(int number) {
            this.number = number;
        }

        /// The numeric aggregation temporality as defined by the protocol.
        @Override
        public int number() {
            return number;
        }

        // Cached to avoid values()'s per-call array clone on the decode hot path.
        private static final AggregationTemporality[] VALUES = values();

        /// Resolves a temporality from its protocol number, falling back to [#UNSPECIFIED].
        public static AggregationTemporality fromNumber(int number) {
            return ProtoEnum.fromNumber(VALUES, number, UNSPECIFIED);
        }
    }

    /// Fluent builder for [Metric]. Fields default to empty/zero; `data` defaults to
    /// the empty [NoData] form.
    public static final class Builder {

        private String name = "";
        private String description = "";
        private String unit = "";
        private Data data = NoData.INSTANCE;
        private Attributes metadata = Attributes.empty();

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder unit(String unit) {
            this.unit = unit;
            return this;
        }

        public Builder data(Data data) {
            this.data = Objects.requireNonNull(data, "data (use Metric.noData() for the empty form)");
            return this;
        }

        public Builder metadata(Attributes metadata) {
            this.metadata = metadata;
            return this;
        }

        public Metric build() {
            return new Metric(name, description, unit, data, metadata);
        }
    }
}
