package dev.nthings.otlp4j.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// A data point for a [Metric.Summary]: a set of quantile values. Mirrors
/// the [Summary] proto message.
public record SummaryPoint(
        Attributes attributes,
        long startEpochNanos,
        long epochNanos,
        long count,
        double sum,
        List<Quantile> quantileValues,
        long flags) {

    public SummaryPoint {
        flags = Ids.flags(flags);
        quantileValues = List.copyOf(quantileValues);
    }

    /// A summary point with no flags set.
    public static SummaryPoint of(
            Attributes attributes,
            long startEpochNanos,
            long epochNanos,
            long count,
            double sum,
            List<Quantile> quantileValues) {
        return new SummaryPoint(attributes, startEpochNanos, epochNanos, count, sum, quantileValues, 0L);
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Returns a pre-populated [Builder] for copy-modify transforms.
    public Builder toBuilder() {
        return new Builder()
                .attributes(attributes)
                .startEpochNanos(startEpochNanos)
                .epochNanos(epochNanos)
                .count(count)
                .sum(sum)
                .quantileValues(quantileValues)
                .flags(flags);
    }

    /// Fluent builder for [SummaryPoint]. Fields default to empty/zero.
    public static final class Builder {

        private Attributes attributes = Attributes.empty();
        private long startEpochNanos;
        private long epochNanos;
        private long count;
        private double sum;
        private final List<Quantile> quantileValues = new ArrayList<>();
        private long flags;

        private Builder() {}

        public Builder attributes(Attributes attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder startEpochNanos(long startEpochNanos) {
            this.startEpochNanos = startEpochNanos;
            return this;
        }

        public Builder epochNanos(long epochNanos) {
            this.epochNanos = epochNanos;
            return this;
        }

        public Builder count(long count) {
            this.count = count;
            return this;
        }

        public Builder sum(double sum) {
            this.sum = sum;
            return this;
        }

        public Builder quantileValues(List<Quantile> quantileValues) {
            Objects.requireNonNull(quantileValues, "quantileValues");
            this.quantileValues.clear();
            this.quantileValues.addAll(quantileValues);
            return this;
        }

        public Builder addQuantileValue(Quantile quantileValue) {
            this.quantileValues.add(quantileValue);
            return this;
        }

        public Builder flags(long flags) {
            this.flags = flags;
            return this;
        }

        public SummaryPoint build() {
            return new SummaryPoint(attributes, startEpochNanos, epochNanos, count, sum, quantileValues, flags);
        }
    }

    /// The value at a given quantile of the distribution.
    public record Quantile(double quantile, double value) {}
}
