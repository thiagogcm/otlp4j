package dev.nthings.otlp4j.core.signal;

/// A connector's signal pairing: the signal it consumes (`from`) and the signal it emits (`to`). A
/// spans-to-metrics connector pairs `SignalDescriptor.TRACES` with `SignalDescriptor.METRICS`. The
/// graph carries a pair as edge metadata and validates it against a connector factory's declared
/// supported pairs.
///
/// @param <I>  the input signal's batch type
/// @param <O>  the output signal's batch type
/// @param from the consumed signal
/// @param to   the emitted signal
public record SignalPair<I, O>(SignalDescriptor<I> from, SignalDescriptor<O> to) {

    /// Validates that neither side is null.
    public SignalPair {
        if (from == null || to == null) {
            throw new IllegalArgumentException("both sides of a signal pair must be non-null");
        }
    }

    @Override
    public String toString() {
        return from + "->" + to;
    }
}
