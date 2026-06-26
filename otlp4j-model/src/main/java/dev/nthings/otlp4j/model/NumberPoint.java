package dev.nthings.otlp4j.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/// A scalar data point for a [Metric.Gauge] or [Metric.Sum]. Mirrors
/// `opentelemetry.proto.metrics.v1.NumberDataPoint`.
///
/// The point's value is a [sealed type][Value] — either an integer or a double. `exemplars`
/// carry the trace/span links sampled for this point (empty when none were recorded).
public record NumberPoint(
        Attributes attributes,
        long startEpochNanos,
        long epochNanos,
        @Nullable Value value,
        long flags,
        List<Exemplar> exemplars) {

    public NumberPoint {
        flags = Ids.flags(flags);
        exemplars = List.copyOf(exemplars);
    }

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

    /// A point with a single timestamp, no start time, and no exemplars.
    public static NumberPoint of(Attributes attributes, long epochNanos, Value value) {
        return of(attributes, 0L, epochNanos, value);
    }

    /// A point with start and observation timestamps and no exemplars.
    public static NumberPoint of(Attributes attributes, long startEpochNanos, long epochNanos, Value value) {
        return new NumberPoint(attributes, startEpochNanos, epochNanos, value, 0L, List.of());
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Fluent builder for [NumberPoint]. Fields default to empty/zero; `value` defaults to
    /// `null`, mirroring a wire point whose value oneof was never set.
    public static final class Builder {

        private Attributes attributes = Attributes.empty();
        private long startEpochNanos;
        private long epochNanos;
        private @Nullable Value value;
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

        public Builder value(Value value) {
            this.value = value;
            return this;
        }

        public Builder longValue(long value) {
            this.value = NumberPoint.longValue(value);
            return this;
        }

        public Builder doubleValue(double value) {
            this.value = NumberPoint.doubleValue(value);
            return this;
        }

        public Builder flags(long flags) {
            this.flags = flags;
            return this;
        }

        public Builder exemplars(List<Exemplar> exemplars) {
            // Null-check before clear() so a bad arg can't half-mutate the builder.
            Objects.requireNonNull(exemplars, "exemplars");
            this.exemplars.clear();
            this.exemplars.addAll(exemplars);
            return this;
        }

        public Builder addExemplar(Exemplar exemplar) {
            this.exemplars.add(exemplar);
            return this;
        }

        public NumberPoint build() {
            return new NumberPoint(attributes, startEpochNanos, epochNanos, value, flags, exemplars);
        }
    }
}
