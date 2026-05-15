package dev.nthings.otlp4j.model;

/// A scalar data point for a [Metric.Gauge] or [Metric.Sum]. Mirrors
/// `opentelemetry.proto.metrics.v1.NumberDataPoint`.
///
/// The point's value is a [sealed type][Value] — either an integer or a double.
public record NumberPoint(
        Attributes attributes, long startEpochNanos, long epochNanos, Value value, long flags) {

    /// A number data point value: either a [LongValue] or a [DoubleValue].
    public sealed interface Value permits LongValue, DoubleValue {}

    public record LongValue(long value) implements Value {}

    public record DoubleValue(double value) implements Value {}

    public static Value longValue(long value) {
        return new LongValue(value);
    }

    public static Value doubleValue(double value) {
        return new DoubleValue(value);
    }
}
