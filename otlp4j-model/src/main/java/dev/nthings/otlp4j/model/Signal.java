package dev.nthings.otlp4j.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/// One of the four OTLP signals, carrying its domain batch type and the batch-level operations the
/// framework performs generically: merge, item count, and an empty batch. The value set is closed
/// (four `public static final` constants; the constructor is private), so `Signal` behaves like a
/// generic enum. Unlike an enum it is parameterised by its batch type `T`, so `merge`, `itemCount`,
/// and `empty` are statically typed with no unchecked casts.
///
/// @param <T> the domain batch type this signal identifies
public final class Signal<T> {

    /// Traces: [TracesData], counted by span.
    public static final Signal<TracesData> TRACES = new Signal<>(
            "traces", TracesData.class,
            Signal::mergeTraces, TracesData::spanCount, () -> new TracesData(List.of()));

    /// Metrics: [MetricsData], counted by data point.
    public static final Signal<MetricsData> METRICS = new Signal<>(
            "metrics", MetricsData.class,
            Signal::mergeMetrics, MetricsData::dataPointCount, () -> new MetricsData(List.of()));

    /// Logs: [LogsData], counted by log record.
    public static final Signal<LogsData> LOGS = new Signal<>(
            "logs", LogsData.class,
            Signal::mergeLogs, LogsData::logRecordCount, () -> new LogsData(List.of()));

    /// Profiles (development signal): [ProfilesData], counted by profile. Merge is only lossless
    /// when batches share one `ProfilesDictionary`; see [#merge].
    public static final Signal<ProfilesData> PROFILES = new Signal<>(
            "profiles", ProfilesData.class,
            Signal::mergeProfiles, ProfilesData::profileCount,
            () -> new ProfilesData(List.of(), new byte[0]));

    private static final List<Signal<?>> VALUES = List.of(TRACES, METRICS, LOGS, PROFILES);

    private final String signalName;
    private final Class<T> batchType;
    private final Merger<T> merger;
    private final ToLongFunction<T> itemCounter;
    private final Supplier<T> empty;

    private Signal(String signalName, Class<T> batchType,
                   Merger<T> merger, ToLongFunction<T> itemCounter, Supplier<T> empty) {
        this.signalName = signalName;
        this.batchType = batchType;
        this.merger = merger;
        this.itemCounter = itemCounter;
        this.empty = empty;
    }

    /// The lowercase signal name (`traces`/`metrics`/`logs`/`profiles`), for logs and diagnostics.
    public String signalName() {
        return signalName;
    }

    /// The domain batch type this signal identifies.
    public Class<T> batchType() {
        return batchType;
    }

    /// Merges a snapshot of batches into one downstream batch. For profiles this throws
    /// [IllegalStateException] if the batches carry distinct non-empty `ProfilesDictionaries`.
    public T merge(List<T> batches) {
        return merger.merge(batches);
    }

    /// The number of OTLP items (spans / data points / log records / profiles) in one batch,
    /// used for partial-success accounting.
    public long itemCount(T batch) {
        return itemCounter.applyAsLong(batch);
    }

    /// An empty batch of this signal.
    public T empty() {
        return empty.get();
    }

    /// The four signals, in wire order (traces, metrics, logs, profiles).
    public static List<Signal<?>> values() {
        return VALUES;
    }

    @Override
    public String toString() {
        return signalName;
    }

    /// Merge strategy over one signal's batches; an implementation detail of [Signal].
    @FunctionalInterface
    private interface Merger<T> {
        T merge(List<T> batches);
    }

    // --- former BatchMergers, now Signal's private implementation detail ---

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
