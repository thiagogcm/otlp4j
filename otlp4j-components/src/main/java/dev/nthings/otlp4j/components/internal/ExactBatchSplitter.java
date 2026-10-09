package dev.nthings.otlp4j.components.internal;

import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// Splits a domain batch into units that satisfy both item and byte caps.
///
/// The signal descriptor owns the lossless item splitter. When a candidate exceeds the byte cap,
/// this helper recursively halves its item count and weighs each resulting payload with the supplied
/// sizer. An exact OTLP sizer measures the complete request envelope and the repeated resource/scope
/// grouping overhead, rather than estimating from the items alone.
///
/// A zero cap means unlimited for that dimension. A non-empty unit whose one indivisible item still
/// exceeds the byte cap cannot be split losslessly and is rejected with an [IllegalArgumentException].
/// A byte cap also needs every unit weighed: a sizer that returns zero or less for a unit holding items
/// cannot enforce the cap, so that is rejected with an [IllegalStateException] rather than let through.
public final class ExactBatchSplitter {

    private ExactBatchSplitter() {}

    /// Splits `data` into payloads bounded by `maxItems` and `maxBytes`.
    ///
    /// @param signal the descriptor owning item counting and lossless splitting
    /// @param data the domain batch to split
    /// @param maxItems the maximum items per result, or `0` for unlimited
    /// @param maxBytes the maximum bytes per result as weighed by `sizer`, or `0` for unlimited
    /// @param sizer weighs each candidate unit; consulted only when `maxBytes` is positive
    /// @param <T> the signal's domain batch type
    /// @return immutable payload units in the original signal order
    /// @throws IllegalArgumentException if a cap is negative or one indivisible item exceeds
    ///     `maxBytes`
    /// @throws IllegalStateException if `maxBytes` is positive and `sizer` does not weigh a unit that
    ///     holds items
    public static <T> List<Payload<T>> split(
            SignalDescriptor<T> signal, T data, int maxItems, long maxBytes, PayloadSizer sizer) {
        Objects.requireNonNull(signal, "signal");
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(sizer, "sizer");
        if (maxItems < 0) {
            throw new IllegalArgumentException("maxItems must be >= 0, got " + maxItems);
        }
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be >= 0, got " + maxBytes);
        }
        if (maxItems == 0 && maxBytes == 0) {
            return List.of(Payload.of(signal, data));
        }

        var result = new ArrayList<Payload<T>>();
        var initial = maxItems > 0 ? signal.split(data, maxItems) : List.of(data);
        for (var candidate : initial) {
            splitCandidate(signal, candidate, maxItems, maxBytes, sizer, result);
        }
        return List.copyOf(result);
    }

    private static <T> void splitCandidate(
            SignalDescriptor<T> signal,
            T candidate,
            int maxItems,
            long maxBytes,
            PayloadSizer sizer,
            List<Payload<T>> result) {
        var payload = Payload.of(signal, candidate);
        var items = signal.itemCount(candidate);
        var withinBytes = true;
        if (maxBytes > 0) {
            var weight = sizer.sizeOf(payload);
            if (weight <= 0 && items > 0) {
                throw new IllegalStateException("the payload sizer returned " + weight + " for a "
                        + signal.signalName() + " unit holding " + items
                        + " items, so the byte cap cannot be enforced");
            }
            withinBytes = weight <= maxBytes;
        }
        if ((maxItems == 0 || items <= maxItems) && withinBytes) {
            result.add(payload);
            return;
        }
        if (items <= 1) {
            throw new IllegalArgumentException(
                    "one indivisible " + signal.signalName() + " item exceeds the configured batch cap");
        }

        var half = Math.max(1L, items / 2);
        for (var part : signal.split(candidate, half)) {
            splitCandidate(signal, part, maxItems, maxBytes, sizer, result);
        }
    }
}
