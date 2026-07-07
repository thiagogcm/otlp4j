package dev.nthings.otlp4j.core.signal;

import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.TracesData;
import java.util.List;

/// The core-side key for one of the four OTLP signals, parameterised by its domain batch type `T`.
/// The value set is closed: four `public static final` constants and a private constructor, so a
/// descriptor behaves like a generic enum, exactly as the model `Signal` does.
///
/// A descriptor owns the signal name, the domain batch type, the wire ordinal (traces `0`, metrics
/// `1`, logs `2`, profiles `3`), whether the signal is a development signal (profiles only), its
/// stability level, and the batch-level operations `merge`, `itemCount`, and `empty`. Those three
/// operations are not reimplemented here: each descriptor delegates to its model `Signal`, so the
/// batch policy is mirrored rather than duplicated and the two can never diverge. Profiles merge, in
/// particular, throws the same `IllegalStateException` on distinct dictionaries that
/// `Signal.PROFILES.merge` throws, which the conformance suite's profiles wire-identical guarantee
/// depends on.
///
/// @param <T> the domain batch type this signal identifies
public final class SignalDescriptor<T> {

    /// Traces, carrying `TracesData`, wire ordinal `0`, stable.
    public static final SignalDescriptor<TracesData> TRACES =
            new SignalDescriptor<>(Signal.TRACES, 0, StabilityLevel.STABLE);

    /// Metrics, carrying `MetricsData`, wire ordinal `1`, stable.
    public static final SignalDescriptor<MetricsData> METRICS =
            new SignalDescriptor<>(Signal.METRICS, 1, StabilityLevel.STABLE);

    /// Logs, carrying `LogsData`, wire ordinal `2`, stable.
    public static final SignalDescriptor<LogsData> LOGS =
            new SignalDescriptor<>(Signal.LOGS, 2, StabilityLevel.STABLE);

    /// Profiles, carrying `ProfilesData`, wire ordinal `3`, a development signal. Merge is only
    /// lossless when batches share one `ProfilesDictionary`; see [#merge(List)].
    public static final SignalDescriptor<ProfilesData> PROFILES =
            new SignalDescriptor<>(Signal.PROFILES, 3, StabilityLevel.DEVELOPMENT);

    private static final List<SignalDescriptor<?>> VALUES = List.of(TRACES, METRICS, LOGS, PROFILES);

    private final Signal<T> signal;
    private final int wireOrdinal;
    private final StabilityLevel stability;

    private SignalDescriptor(Signal<T> signal, int wireOrdinal, StabilityLevel stability) {
        this.signal = signal;
        this.wireOrdinal = wireOrdinal;
        this.stability = stability;
    }

    /// The lowercase signal name (`traces`/`metrics`/`logs`/`profiles`), for logs and diagnostics.
    ///
    /// @return the signal name
    public String signalName() {
        return signal.signalName();
    }

    /// The domain batch type this signal identifies.
    ///
    /// @return the batch class
    public Class<T> batchType() {
        return signal.batchType();
    }

    /// The signal's position in wire order: traces `0`, metrics `1`, logs `2`, profiles `3`.
    ///
    /// @return the wire ordinal
    public int wireOrdinal() {
        return wireOrdinal;
    }

    /// Whether this is a development signal (profiles only). Equivalent to a `DEVELOPMENT` stability.
    ///
    /// @return true for profiles, false otherwise
    public boolean development() {
        return stability == StabilityLevel.DEVELOPMENT;
    }

    /// The signal's declared stability level.
    ///
    /// @return the stability level
    public StabilityLevel stability() {
        return stability;
    }

    /// Merges a snapshot of batches into one downstream batch, delegating to the model `Signal`. For
    /// profiles this throws `IllegalStateException` if the batches carry distinct non-empty
    /// `ProfilesDictionaries`.
    ///
    /// @param batches the batches to merge
    /// @return the merged batch
    public T merge(List<T> batches) {
        return signal.merge(batches);
    }

    /// The number of OTLP items (spans / data points / log records / profiles) in one batch, used for
    /// partial-success accounting; delegates to the model `Signal`.
    ///
    /// @param batch the batch to count
    /// @return the item count
    public long itemCount(T batch) {
        return signal.itemCount(batch);
    }

    /// An empty batch of this signal, delegating to the model `Signal`.
    ///
    /// @return an empty batch
    public T empty() {
        return signal.empty();
    }

    /// The four signal descriptors, in wire order (traces, metrics, logs, profiles).
    ///
    /// @return the immutable list of descriptors
    public static List<SignalDescriptor<?>> values() {
        return VALUES;
    }

    @Override
    public String toString() {
        return signal.signalName();
    }
}
