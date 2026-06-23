package dev.nthings.otlp4j.model;

import java.util.Objects;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/// An exemplar attached to a metric data point. Mirrors
/// `opentelemetry.proto.metrics.v1.Exemplar`.
///
/// An exemplar links a single metric measurement to the trace/span it was recorded under,
/// letting a backend pivot from an aggregate series to a concrete sampled request.
/// `filteredAttributes` are the key/values the aggregator dropped when collapsing the
/// measurement into its series but kept on the exemplar. `spanId`/`traceId` are
/// lowercase-hex strings — empty when the measurement was not recorded inside a sampled
/// trace, and rejected at construction time if not valid hex. `value` reuses
/// [NumberPoint.Value]; it may be null for an invalid exemplar whose wire `value` oneof
/// recognized neither an integer nor a double.
@NullMarked
public record Exemplar(
        Attributes filteredAttributes,
        long epochNanos,
        NumberPoint.@Nullable Value value,
        String spanId,
        String traceId) {

    public Exemplar {
        Objects.requireNonNull(filteredAttributes, "filteredAttributes");
        spanId = Ids.spanId(spanId);
        traceId = Ids.traceId(traceId);
    }

    /// An exemplar not linked to a sampled trace (empty `spanId`/`traceId`).
    public static Exemplar of(Attributes filteredAttributes, long epochNanos, NumberPoint.@Nullable Value value) {
        return new Exemplar(filteredAttributes, epochNanos, value, "", "");
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Fluent builder for [Exemplar]. Fields default to empty/zero; `value` defaults to `null`,
    /// preserving the round-trip of an exemplar whose wire value oneof was unset.
    public static final class Builder {

        private Attributes filteredAttributes = Attributes.empty();
        private long epochNanos;
        private NumberPoint.@Nullable Value value;
        private String spanId = "";
        private String traceId = "";

        private Builder() {}

        public Builder filteredAttributes(Attributes filteredAttributes) {
            this.filteredAttributes = filteredAttributes;
            return this;
        }

        public Builder epochNanos(long epochNanos) {
            this.epochNanos = epochNanos;
            return this;
        }

        public Builder value(NumberPoint.@Nullable Value value) {
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

        public Builder spanId(String spanId) {
            this.spanId = spanId;
            return this;
        }

        public Builder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public Exemplar build() {
            return new Exemplar(filteredAttributes, epochNanos, value, spanId, traceId);
        }
    }
}
