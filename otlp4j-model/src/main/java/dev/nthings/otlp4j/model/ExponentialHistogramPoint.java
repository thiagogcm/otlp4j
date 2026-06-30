package dev.nthings.otlp4j.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;

/// A data point for a [Metric.ExponentialHistogram]: a base-2 exponential bucket
/// distribution. Mirrors the [ExponentialHistogram] proto message.
///
/// Prefer [#builder()] over the positional constructor.
public record ExponentialHistogramPoint(
        Attributes attributes,
        long startEpochNanos,
        long epochNanos,
        long count,
        OptionalDouble sum,
        int scale,
        long zeroCount,
        Buckets positive,
        Buckets negative,
        OptionalDouble min,
        OptionalDouble max,
        double zeroThreshold,
        long flags,
        List<Exemplar> exemplars) {

    public ExponentialHistogramPoint {
        flags = Ids.flags(flags);
        exemplars = List.copyOf(exemplars);
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Returns a pre-populated [Builder] for copy-modify transforms.
    public Builder toBuilder() {
        Builder builder = new Builder()
                .attributes(attributes)
                .startEpochNanos(startEpochNanos)
                .epochNanos(epochNanos)
                .count(count)
                .scale(scale)
                .zeroCount(zeroCount)
                .positive(positive)
                .negative(negative)
                .zeroThreshold(zeroThreshold)
                .flags(flags)
                .exemplars(exemplars);
        // Empty OptionalDoubles map to "unset"; the builder defaults to empty for each.
        sum.ifPresent(builder::sum);
        min.ifPresent(builder::min);
        max.ifPresent(builder::max);
        return builder;
    }

    /// A contiguous run of exponential bucket counts starting at `offset`.
    public record Buckets(int offset, List<Long> bucketCounts) {

        public static final Buckets EMPTY = new Buckets(0, List.of());

        public Buckets {
            bucketCounts = List.copyOf(bucketCounts);
        }
    }

    /// Fluent builder for [ExponentialHistogramPoint]. Fields default to empty/zero;
    /// `positive`/`negative` default to [Buckets#EMPTY].
    public static final class Builder {

        private Attributes attributes = Attributes.empty();
        private long startEpochNanos;
        private long epochNanos;
        private long count;
        private OptionalDouble sum = OptionalDouble.empty();
        private int scale;
        private long zeroCount;
        private Buckets positive = Buckets.EMPTY;
        private Buckets negative = Buckets.EMPTY;
        private OptionalDouble min = OptionalDouble.empty();
        private OptionalDouble max = OptionalDouble.empty();
        private double zeroThreshold;
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

        public Builder scale(int scale) {
            this.scale = scale;
            return this;
        }

        public Builder zeroCount(long zeroCount) {
            this.zeroCount = zeroCount;
            return this;
        }

        public Builder positive(Buckets positive) {
            this.positive = Objects.requireNonNull(positive, "positive");
            return this;
        }

        public Builder negative(Buckets negative) {
            this.negative = Objects.requireNonNull(negative, "negative");
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

        public Builder zeroThreshold(double zeroThreshold) {
            this.zeroThreshold = zeroThreshold;
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

        public ExponentialHistogramPoint build() {
            return new ExponentialHistogramPoint(
                    attributes,
                    startEpochNanos,
                    epochNanos,
                    count,
                    sum,
                    scale,
                    zeroCount,
                    positive,
                    negative,
                    min,
                    max,
                    zeroThreshold,
                    flags,
                    exemplars);
        }
    }
}
