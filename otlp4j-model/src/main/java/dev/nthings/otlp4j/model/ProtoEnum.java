package dev.nthings.otlp4j.model;

/// An enum constant with an explicit OTLP wire number, decoupled from declaration order.
/// Package-private unifying contract for the model's wire enums ([Span.Kind],
/// [Span.Status.Code], [Metric.AggregationTemporality], [LogRecord.Severity]).
interface ProtoEnum {

    /// The numeric value this constant encodes to on the wire.
    int number();

    /// Resolves the constant in `values` whose [#number()] equals `number`, falling back to
    /// `fallback` for an unrecognized value.
    static <E extends Enum<E> & ProtoEnum> E fromNumber(E[] values, int number, E fallback) {
        for (var value : values) {
            if (value.number() == number) {
                return value;
            }
        }
        return fallback;
    }
}
