package dev.nthings.otlp4j.model;

/// An exemplar attached to a metric data point. Mirrors
/// `opentelemetry.proto.metrics.v1.Exemplar`.
///
/// An exemplar links a single metric measurement to the trace/span it was recorded under,
/// letting a backend pivot from an aggregate series to a concrete sampled request.
/// `filteredAttributes` are the key/values the aggregator dropped when collapsing the
/// measurement into its series but kept on the exemplar. `spanId`/`traceId` are
/// lowercase-hex strings — empty when the measurement was not recorded inside a sampled
/// trace, and rejected when encoded to the wire if not valid hex. `value` reuses
/// [NumberPoint.Value]; it may be null for an invalid exemplar whose wire `value` oneof
/// recognized neither an integer nor a double.
public record Exemplar(
        Attributes filteredAttributes,
        long epochNanos,
        NumberPoint.Value value,
        String spanId,
        String traceId) {}
