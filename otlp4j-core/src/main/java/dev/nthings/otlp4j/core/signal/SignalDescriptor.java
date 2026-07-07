package dev.nthings.otlp4j.core.signal;

import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/// The core-side key for one of the four OTLP signals, parameterised by its domain batch type `T`.
/// The value set is closed: four `public static final` constants and a private constructor, so a
/// descriptor behaves like a generic enum.
///
/// A descriptor owns the signal name, the domain batch type, the wire ordinal (traces `0`, metrics
/// `1`, logs `2`, profiles `3`), whether the signal is a development signal (profiles only), its
/// stability level, and the batch-level operations `merge`, `itemCount`, and `empty`. Those three
/// operations are implemented here directly over the immutable model records, so the batch policy is
/// core behaviour rather than model behaviour. Profiles merge, in particular, throws an
/// `IllegalStateException` on distinct non-empty dictionaries, which the conformance suite's profiles
/// wire-identical guarantee depends on.
///
/// @param <T> the domain batch type this signal identifies
public final class SignalDescriptor<T> {

    /// Traces, carrying `TracesData`, wire ordinal `0`, stable.
    public static final SignalDescriptor<TracesData> TRACES = new SignalDescriptor<>(
            "traces", TracesData.class, SignalDescriptor::mergeTraces, TracesData::spanCount,
            () -> new TracesData(List.of()), 0, StabilityLevel.STABLE);

    /// Metrics, carrying `MetricsData`, wire ordinal `1`, stable.
    public static final SignalDescriptor<MetricsData> METRICS = new SignalDescriptor<>(
            "metrics", MetricsData.class, SignalDescriptor::mergeMetrics, MetricsData::dataPointCount,
            () -> new MetricsData(List.of()), 1, StabilityLevel.STABLE);

    /// Logs, carrying `LogsData`, wire ordinal `2`, stable.
    public static final SignalDescriptor<LogsData> LOGS = new SignalDescriptor<>(
            "logs", LogsData.class, SignalDescriptor::mergeLogs, LogsData::logRecordCount,
            () -> new LogsData(List.of()), 2, StabilityLevel.STABLE);

    /// Profiles, carrying `ProfilesData`, wire ordinal `3`, a development signal. Merge is only
    /// lossless when batches share one `ProfilesDictionary`; see [#merge(List)].
    public static final SignalDescriptor<ProfilesData> PROFILES = new SignalDescriptor<>(
            "profiles", ProfilesData.class, SignalDescriptor::mergeProfiles, ProfilesData::profileCount,
            () -> new ProfilesData(List.of(), new byte[0]), 3, StabilityLevel.DEVELOPMENT);

    private static final List<SignalDescriptor<?>> VALUES = List.of(TRACES, METRICS, LOGS, PROFILES);

    private final String signalName;
    private final Class<T> batchType;
    private final Merger<T> merger;
    private final ToLongFunction<T> itemCounter;
    private final Supplier<T> empty;
    private final int wireOrdinal;
    private final StabilityLevel stability;

    private SignalDescriptor(String signalName, Class<T> batchType, Merger<T> merger,
                             ToLongFunction<T> itemCounter, Supplier<T> empty,
                             int wireOrdinal, StabilityLevel stability) {
        this.signalName = signalName;
        this.batchType = batchType;
        this.merger = merger;
        this.itemCounter = itemCounter;
        this.empty = empty;
        this.wireOrdinal = wireOrdinal;
        this.stability = stability;
    }

    /// The lowercase signal name (`traces`/`metrics`/`logs`/`profiles`), for logs and diagnostics.
    ///
    /// @return the signal name
    public String signalName() {
        return signalName;
    }

    /// The domain batch type this signal identifies.
    ///
    /// @return the batch class
    public Class<T> batchType() {
        return batchType;
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

    /// Merges a snapshot of batches into one downstream batch. For profiles this throws
    /// `IllegalStateException` if the batches carry distinct non-empty `ProfilesDictionaries`.
    ///
    /// @param batches the batches to merge
    /// @return the merged batch
    public T merge(List<T> batches) {
        return merger.merge(batches);
    }

    /// The number of OTLP items (spans / data points / log records / profiles) in one batch, used for
    /// partial-success accounting.
    ///
    /// @param batch the batch to count
    /// @return the item count
    public long itemCount(T batch) {
        return itemCounter.applyAsLong(batch);
    }

    /// An empty batch of this signal.
    ///
    /// @return an empty batch
    public T empty() {
        return empty.get();
    }

    /// The four signal descriptors, in wire order (traces, metrics, logs, profiles).
    ///
    /// @return the immutable list of descriptors
    public static List<SignalDescriptor<?>> values() {
        return VALUES;
    }

    @Override
    public String toString() {
        return signalName;
    }

    /// Merge strategy over one signal's batches; an implementation detail of [SignalDescriptor].
    @FunctionalInterface
    private interface Merger<T> {
        T merge(List<T> batches);
    }

    // --- per-signal merge bodies, batch policy owned by core ---

    private static TracesData mergeTraces(List<TracesData> snapshot) {
        var combined = new ArrayList<TracesData.ResourceSpans>();
        snapshot.forEach(t -> combined.addAll(t.resourceSpans()));
        return new TracesData(combined);
    }

    private static MetricsData mergeMetrics(List<MetricsData> snapshot) {
        var combined = new ArrayList<MetricsData.ResourceMetrics>();
        snapshot.forEach(m -> combined.addAll(m.resourceMetrics()));
        return new MetricsData(combined);
    }

    private static LogsData mergeLogs(List<LogsData> snapshot) {
        var combined = new ArrayList<LogsData.ResourceLogs>();
        snapshot.forEach(l -> combined.addAll(l.resourceLogs()));
        return new LogsData(combined);
    }

    private static ProfilesData mergeProfiles(List<ProfilesData> snapshot) {
        var combined = new ArrayList<ProfilesData.ResourceProfiles>();
        var dictionary = new byte[0];
        for (var p : snapshot) {
            combined.addAll(p.resourceProfiles());
            var dict = p.dictionary();
            if (dict.length == 0) {
                continue;
            }
            if (dictionary.length == 0) {
                dictionary = dict;
            } else if (!Arrays.equals(dictionary, dict)) {
                throw new IllegalStateException(
                        "cannot batch-merge profiles carrying distinct ProfilesDictionaries without "
                                + "re-indexing; forward profiles 1:1 or disable profiles batching");
            }
        }
        return new ProfilesData(combined, dictionary);
    }
}
