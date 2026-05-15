package dev.nthings.otlp4j.model;

import java.util.List;

/// A named metric and its time series. Mirrors `opentelemetry.proto.metrics.v1.Metric`.
///
/// The `data` field is a [sealed type][Data] for gauge, sum, histogram, exponential histogram,
/// and summary payloads.
public record Metric(String name, String description, String unit, Data data, Attributes metadata) {

    public static Builder builder() {
        return new Builder();
    }

    /// The aggregation kind of a metric and its data points.
    public sealed interface Data permits Gauge, Sum, Histogram, ExponentialHistogram, Summary {}

    public record Gauge(List<NumberPoint> points) implements Data {
        public Gauge {
            points = List.copyOf(points);
        }
    }

    public record Sum(List<NumberPoint> points, AggregationTemporality temporality, boolean monotonic)
            implements Data {
        public Sum {
            points = List.copyOf(points);
        }
    }

    public record Histogram(List<HistogramPoint> points, AggregationTemporality temporality)
            implements Data {
        public Histogram {
            points = List.copyOf(points);
        }
    }

    public record ExponentialHistogram(
            List<ExponentialHistogramPoint> points, AggregationTemporality temporality)
            implements Data {
        public ExponentialHistogram {
            points = List.copyOf(points);
        }
    }

    public record Summary(List<SummaryPoint> points) implements Data {
        public Summary {
            points = List.copyOf(points);
        }
    }

    /// How a metric aggregator reports values relative to time.
    public enum AggregationTemporality {
        UNSPECIFIED,
        DELTA,
        CUMULATIVE
    }

    /// Fluent builder for [Metric]. `name`/`description`/`unit` default to
    /// empty strings and `metadata` to [Attributes#empty()]; `data` must be set.
    public static final class Builder {

        private String name = "";
        private String description = "";
        private String unit = "";
        private Data data;
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
            this.data = data;
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
