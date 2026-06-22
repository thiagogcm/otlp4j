package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.OptionalDouble;

/// A data point for a [Metric.ExponentialHistogram]: a base-2 exponential bucket
/// distribution. Mirrors `opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint`.
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
        exemplars = List.copyOf(exemplars);
    }

    /// A contiguous run of exponential bucket counts starting at `offset`.
    public record Buckets(int offset, List<Long> bucketCounts) {

        public static final Buckets EMPTY = new Buckets(0, List.of());

        public Buckets {
            bucketCounts = List.copyOf(bucketCounts);
        }
    }
}
