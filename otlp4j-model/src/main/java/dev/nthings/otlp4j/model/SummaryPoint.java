package dev.nthings.otlp4j.model;

import java.util.List;

/// A data point for a [Metric.Summary]: a set of quantile values. Mirrors
/// `opentelemetry.proto.metrics.v1.SummaryDataPoint`.
public record SummaryPoint(
        Attributes attributes,
        long startEpochNanos,
        long epochNanos,
        long count,
        double sum,
        List<Quantile> quantileValues,
        long flags) {

    public SummaryPoint {
        quantileValues = List.copyOf(quantileValues);
    }

    /// The value at a given quantile of the distribution.
    public record Quantile(double quantile, double value) {}
}
