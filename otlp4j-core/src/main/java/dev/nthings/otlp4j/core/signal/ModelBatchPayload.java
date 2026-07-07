package dev.nthings.otlp4j.core.signal;

/// The interim telemetry carrier that flows through the graph in the first milestone: it wraps one
/// current domain batch (a `TracesData` and friends) together with the [SignalDescriptor] that keys
/// it, answering the two [TelemetryPayload] methods from the descriptor. It lives in core so the
/// single model-batch-carrying payload is reachable by every module that must wrap or unwrap it: the
/// service graph, the transport receiver and exporter adapters, and the sample seed components. Core
/// already names the domain batch types through [SignalDescriptor], so a concrete carrier over them
/// adds no new dependency.
///
/// This is a deliberate seam. Because every graph contract is expressed over `TelemetryPayload` and
/// `SignalConsumer`, and never over this concrete type, a later phase replaces this class with a
/// proto-carrying payload and deletes it without changing any graph contract. The wrapped batch is an
/// immutable model record, so the same instance can be handed to several fan-out peers without a
/// defensive copy; the copy hook the fan-out node reserves for a mutating consumer lands with the
/// payload widening that a later phase brings.
///
/// @param <T> the domain batch type wrapped
public final class ModelBatchPayload<T> implements TelemetryPayload {

    private final SignalDescriptor<T> signal;

    private final T batch;

    /// Wraps `batch` under `signal`.
    ///
    /// @param signal the signal that keys the batch
    /// @param batch  the domain batch
    public ModelBatchPayload(SignalDescriptor<T> signal, T batch) {
        if (signal == null) {
            throw new IllegalArgumentException("signal must be non-null");
        }
        if (batch == null) {
            throw new IllegalArgumentException("batch must be non-null");
        }
        this.signal = signal;
        this.batch = batch;
    }

    /// The wrapped domain batch.
    ///
    /// @return the batch
    public T batch() {
        return batch;
    }

    @Override
    public SignalDescriptor<?> signal() {
        return signal;
    }

    @Override
    public long itemCount() {
        return signal.itemCount(batch);
    }

    @Override
    public String toString() {
        return "ModelBatchPayload(" + signal.signalName() + ", items=" + itemCount() + ")";
    }
}
