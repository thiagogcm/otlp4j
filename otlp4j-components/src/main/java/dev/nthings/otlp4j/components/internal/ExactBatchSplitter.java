package dev.nthings.otlp4j.components.internal;

import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// Splits a domain batch into units that satisfy both item and byte caps.
///
/// The signal descriptor owns the lossless item splitter. When a candidate exceeds the byte cap, this
/// helper splits it and weighs each part with the supplied sizer, recursing into any that still exceeds
/// the cap. Weighing can mean encoding, so a part aims at the item count the candidate's weight suggests
/// would fit, which saves the encodes repeated halving would spend on the same items. It never aims
/// above half, because a part can weigh more than its share when items share overhead such as a
/// profiles dictionary. An exact OTLP sizer measures the complete request envelope and the repeated
/// resource/scope grouping overhead, rather than estimating from the items alone.
///
/// A zero cap means unlimited for that dimension. A non-empty unit whose one indivisible item still
/// exceeds the byte cap cannot be split losslessly and is rejected with an [IllegalArgumentException].
/// So is a batch whose split would weigh more than [#MAX_SPLIT_COST] times the batch: every unit repeats
/// its resource, scope, and any overhead its items share, so when those nearly fill the cap each unit
/// holds few items and the units together dwarf the batch.
/// A byte cap also needs every unit weighed: a sizer that returns zero or less for a unit holding items
/// cannot enforce the cap, so that is rejected with an [IllegalStateException] rather than let through.
public final class ExactBatchSplitter {

    /// How many times its own weight a batch may cost to split. An ordinary split weighs about twice
    /// its batch; items sharing overhead that takes up most of the cap can approach this.
    private static final int MAX_SPLIT_COST = 64;

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
    /// @throws IllegalArgumentException if a cap is negative, one indivisible item exceeds `maxBytes`,
    ///     or the split would cost too much
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
            var payload = Payload.of(signal, candidate);
            var weight = maxBytes > 0 ? sizer.sizeOf(payload) : 0;
            splitWeighed(payload, weight, new SplitBudget(weight), maxItems, maxBytes, sizer, result);
        }
        return List.copyOf(result);
    }

    /// Confirms `payload`, which `sizer` has already weighed at `weight`, can be split the way
    /// [#split] would split it on its own, and counts the units. A payload within both caps needs no
    /// further weighing.
    ///
    /// @param payload the payload to check
    /// @param weight its weight according to `sizer`
    /// @param maxItems the maximum items per unit, or `0` for unlimited
    /// @param maxBytes the maximum bytes per unit, or `0` for unlimited
    /// @param sizer weighs the parts of a payload above the cap
    /// @param <T> the signal's domain batch type
    /// @return the number of units `payload` splits into
    /// @throws IllegalArgumentException if one indivisible item exceeds `maxBytes`, or the split would
    ///     cost too much
    /// @throws IllegalStateException if `sizer` does not weigh a unit that holds items
    public static <T> int requireFits(
            Payload<T> payload, long weight, int maxItems, long maxBytes, PayloadSizer sizer) {
        if (maxItems > 0 && payload.itemCount() > maxItems) {
            return split(payload.signal(), payload.data(), maxItems, maxBytes, sizer).size();
        }
        var units = new ArrayList<Payload<T>>();
        splitWeighed(payload, weight, new SplitBudget(weight), 0, maxBytes, sizer, units);
        return units.size();
    }

    private static <T> void splitWeighed(
            Payload<T> payload,
            long weight,
            SplitBudget budget,
            int maxItems,
            long maxBytes,
            PayloadSizer sizer,
            List<Payload<T>> result) {
        var signal = payload.signal();
        var items = payload.itemCount();
        if (maxBytes > 0 && weight <= 0 && items > 0) {
            throw new IllegalStateException("the payload sizer returned " + weight + " for a "
                    + signal.signalName() + " unit holding " + items
                    + " items, so the byte cap cannot be enforced");
        }
        var withinBytes = maxBytes == 0 || weight <= maxBytes;
        if ((maxItems == 0 || items <= maxItems) && withinBytes) {
            result.add(payload);
            return;
        }
        if (items <= 1) {
            throw new IllegalArgumentException(
                    "one indivisible " + signal.signalName() + " item exceeds the configured batch cap");
        }

        var part = Math.max(1L, items / 2);
        if (!withinBytes) {
            part = Math.max(1L, Math.min(part, (long) ((double) items * maxBytes / weight)));
        }
        for (var unit : signal.split(payload.data(), part)) {
            var candidate = Payload.of(signal, unit);
            var unitWeight = maxBytes > 0 ? budget.spend(sizer.sizeOf(candidate)) : 0;
            splitWeighed(candidate, unitWeight, budget, maxItems, maxBytes, sizer, result);
        }
    }

    /// The weight one batch's split has left to spend.
    private static final class SplitBudget {

        private long remaining;

        private SplitBudget(long batchWeight) {
            remaining = batchWeight * MAX_SPLIT_COST;
        }

        private long spend(long weight) {
            remaining -= weight;
            if (remaining < 0) {
                throw new IllegalArgumentException("splitting a batch to fit the byte cap would weigh over "
                        + MAX_SPLIT_COST + " times the batch, because its grouping fills most of each unit");
            }
            return weight;
        }
    }
}
