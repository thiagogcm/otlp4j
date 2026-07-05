package dev.nthings.otlp4j.model;

import java.util.Objects;

/// A batch paired with the [Signal] that identifies it: the generic carrier that lets code move a
/// "some batch of some signal" value (`SignalBatch<?>`) through one export/dispatch path instead of
/// four, and recover the signal's merge/count/empty operations without a per-signal switch.
///
/// @param signal the signal this batch belongs to
/// @param batch  the domain batch
/// @param <T>    the domain batch type
public record SignalBatch<T>(Signal<T> signal, T batch) {

    public SignalBatch {
        Objects.requireNonNull(signal, "signal");
        Objects.requireNonNull(batch, "batch");
    }

    /// The item count of the carried batch (`signal.itemCount(batch)`).
    public long itemCount() {
        return signal.itemCount(batch);
    }
}
