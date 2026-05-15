package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.OptionalDouble;

/// A data point for a [Metric.Histogram]: an explicit-bounds bucket distribution. Mirrors
/// `opentelemetry.proto.metrics.v1.HistogramDataPoint`.
///
/// `bucketCounts` has exactly one more element than `explicitBounds` (or both are
/// empty when only `count`/`sum` are known).
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
        long flags) {

    public HistogramPoint {
        bucketCounts = List.copyOf(bucketCounts);
        explicitBounds = List.copyOf(explicitBounds);
    }
}
