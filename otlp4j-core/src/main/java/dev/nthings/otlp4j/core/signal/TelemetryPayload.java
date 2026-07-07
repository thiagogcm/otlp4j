package dev.nthings.otlp4j.core.signal;

/// The minimal marker for a unit of telemetry flowing through the graph. Phase 1 keeps this narrow
/// on purpose: it exposes only the signal it belongs to and its item count, which is all the graph,
/// the consumers, and the runtime need. Every graph contract is expressed over
/// `SignalConsumer<T extends TelemetryPayload>` and `SignalDescriptor<T>`, so the concrete payload
/// type is never named in a factory, a consumer, the graph, or the runtime.
///
/// This is the seam that lets a later phase widen the marker with serialization and view methods and
/// swap in a proto-carrying implementation without touching any graph contract.
public interface TelemetryPayload {

    /// The signal this payload belongs to.
    ///
    /// @return the signal descriptor
    SignalDescriptor<?> signal();

    /// The number of OTLP items in this payload, for partial-success accounting.
    ///
    /// @return the item count
    long itemCount();
}
