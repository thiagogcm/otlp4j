package dev.nthings.otlp4j.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;

/// A data point for a [Metric.Histogram]: an explicit-bounds bucket distribution. Mirrors
/// `opentelemetry.proto.metrics.v1.HistogramDataPoint`.
///
/// `bucketCounts` has exactly one more element than `explicitBounds` (or both are
/// empty when only `count`/`sum` are known); this invariant is enforced at construction.
/// Prefer [#builder()] over the positional constructor.
public record HistogramPoint(
        Attributes attributes,
        long startEpochNanos,
        long epochNanos,
        long count,
        OptionalDouble sum,
        List<Long> bucketCounts,
        List<Double> explicitBounds,
        OptionalDouble min,
        OptionalDouble max,
        long flags,
        List<Exemplar> exemplars) {

    public HistogramPoint {
        bucketCounts = List.copyOf(bucketCounts);
        explicitBounds = List.copyOf(explicitBounds);
        exemplars = List.copyOf(exemplars);
        // One more bucket than bound, or both empty for a count/sum-only point.
        if (!(bucketCounts.isEmpty() && explicitBounds.isEmpty())
                && bucketCounts.size() != explicitBounds.size() + 1) {
            throw new IllegalArgumentException(
                    "bucketCounts.size() (" + bucketCounts.size()
                            + ") must equal explicitBounds.size() (" + explicitBounds.size() + ") + 1");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Fluent builder for [HistogramPoint]. Optional fields default to empty/zero; [#build()]
    /// enforces the bucket invariant at construction.
    public static final class Builder {

        private Attributes attributes = Attributes.empty();
        private long startEpochNanos;
        private long epochNanos;
        private long count;
        private OptionalDouble sum = OptionalDouble.empty();
        private final List<Long> bucketCounts = new ArrayList<>();
        private final List<Double> explicitBounds = new ArrayList<>();
        private OptionalDouble min = OptionalDouble.empty();
        private OptionalDouble max = OptionalDouble.empty();
        private long flags;
        private final List<Exemplar> exemplars = new ArrayList<>();

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
            this.sum = OptionalDouble.of(sum);
            return this;
        }

        public Builder bucketCounts(List<Long> bucketCounts) {
            // Null-check before clear() so a bad arg can't half-mutate the builder.
            Objects.requireNonNull(bucketCounts, "bucketCounts");
            this.bucketCounts.clear();
            this.bucketCounts.addAll(bucketCounts);
            return this;
        }

        public Builder addBucketCount(long bucketCount) {
            this.bucketCounts.add(bucketCount);
            return this;
        }

        public Builder explicitBounds(List<Double> explicitBounds) {
            Objects.requireNonNull(explicitBounds, "explicitBounds");
            this.explicitBounds.clear();
            this.explicitBounds.addAll(explicitBounds);
            return this;
        }

        public Builder addExplicitBound(double explicitBound) {
            this.explicitBounds.add(explicitBound);
            return this;
        }

        public Builder min(double min) {
            this.min = OptionalDouble.of(min);
            return this;
        }

        public Builder max(double max) {
            this.max = OptionalDouble.of(max);
            return this;
        }

        public Builder flags(long flags) {
            this.flags = flags;
            return this;
        }

        public Builder exemplars(List<Exemplar> exemplars) {
            Objects.requireNonNull(exemplars, "exemplars");
            this.exemplars.clear();
            this.exemplars.addAll(exemplars);
            return this;
        }

        public Builder addExemplar(Exemplar exemplar) {
            this.exemplars.add(exemplar);
            return this;
        }

        public HistogramPoint build() {
            return new HistogramPoint(
                    attributes,
                    startEpochNanos,
                    epochNanos,
                    count,
                    sum,
                    bucketCounts,
                    explicitBounds,
                    min,
                    max,
                    flags,
                    exemplars);
        }
    }
}
