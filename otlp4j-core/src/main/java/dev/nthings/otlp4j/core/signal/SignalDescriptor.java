package dev.nthings.otlp4j.core.signal;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
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
/// `1`, logs `2`, profiles `3`), and the batch-level operations `merge`, `itemCount`, and `empty`.
/// Those three operations are implemented here directly over the immutable model records, so the batch policy is
/// core behaviour rather than model behaviour. Profiles merge, in particular, throws an
/// `IllegalStateException` on distinct non-empty dictionaries, which the conformance suite's profiles
/// wire-identical guarantee depends on.
///
/// @param <T> the domain batch type this signal identifies
public final class SignalDescriptor<T> {

    /// Traces, carrying `TracesData`, wire ordinal `0`.
    public static final SignalDescriptor<TracesData> TRACES = new SignalDescriptor<>(
            "traces", TracesData.class, SignalDescriptor::mergeTraces, SignalDescriptor::splitTraces,
            TracesData::spanCount, () -> new TracesData(List.of()), 0);

    /// Metrics, carrying `MetricsData`, wire ordinal `1`.
    public static final SignalDescriptor<MetricsData> METRICS = new SignalDescriptor<>(
            "metrics", MetricsData.class, SignalDescriptor::mergeMetrics, SignalDescriptor::splitMetrics,
            MetricsData::dataPointCount, () -> new MetricsData(List.of()), 1);

    /// Logs, carrying `LogsData`, wire ordinal `2`.
    public static final SignalDescriptor<LogsData> LOGS = new SignalDescriptor<>(
            "logs", LogsData.class, SignalDescriptor::mergeLogs, SignalDescriptor::splitLogs,
            LogsData::logRecordCount, () -> new LogsData(List.of()), 2);

    /// Profiles, carrying `ProfilesData`, wire ordinal `3`. Merge is only lossless when batches share
    /// one `ProfilesDictionary`; see [#merge(List)]. Split, by contrast, only slices the profile list
    /// under the batch's single shared dictionary, so it is always lossless.
    public static final SignalDescriptor<ProfilesData> PROFILES = new SignalDescriptor<>(
            "profiles", ProfilesData.class, SignalDescriptor::mergeProfiles, SignalDescriptor::splitProfiles,
            ProfilesData::profileCount, () -> new ProfilesData(List.of(), new byte[0]), 3);

    private static final List<SignalDescriptor<?>> VALUES = List.of(TRACES, METRICS, LOGS, PROFILES);

    private final String signalName;
    private final Class<T> batchType;
    private final Merger<T> merger;
    private final Splitter<T> splitter;
    private final ToLongFunction<T> itemCounter;
    private final Supplier<T> empty;
    private final int wireOrdinal;

    private SignalDescriptor(String signalName, Class<T> batchType, Merger<T> merger,
                             Splitter<T> splitter, ToLongFunction<T> itemCounter, Supplier<T> empty,
                             int wireOrdinal) {
        this.signalName = signalName;
        this.batchType = batchType;
        this.merger = merger;
        this.splitter = splitter;
        this.itemCounter = itemCounter;
        this.empty = empty;
        this.wireOrdinal = wireOrdinal;
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

    /// Merges a snapshot of batches into one downstream batch. For profiles this throws
    /// `IllegalStateException` if the batches carry distinct non-empty `ProfilesDictionaries`.
    ///
    /// @param batches the batches to merge
    /// @return the merged batch
    public T merge(List<T> batches) {
        return merger.merge(batches);
    }

    /// Partitions one batch into sub-batches, each holding no more than `maxItems` OTLP items (spans,
    /// data points, log records, or profiles), the inverse of [#merge(List)]. The partition is
    /// item-count based and preserves each item's resource and scope grouping, so resource-level
    /// attributes survive on every sub-batch. Items are emitted in wire order, and the item totals
    /// across the returned sub-batches equal the input's item count.
    ///
    /// A batch already at or under the cap is returned as a singleton list of that same batch, with no
    /// copy. Unlike merge, split is lossless for every signal including profiles, because it only
    /// slices the item lists under the batch's single shared dictionary rather than reconciling
    /// distinct dictionaries. This is the generic mechanism a batch processor uses to honour a
    /// send-batch maximum size without a per-signal switch.
    ///
    /// @param batch    the batch to partition
    /// @param maxItems the maximum item count per sub-batch, which must be positive
    /// @return the sub-batches, in wire order, each with at most `maxItems` items
    public List<T> split(T batch, long maxItems) {
        if (maxItems <= 0) {
            throw new IllegalArgumentException("maxItems must be positive, was " + maxItems);
        }
        if (itemCount(batch) <= maxItems) {
            return List.of(batch);
        }
        return splitter.split(batch, maxItems);
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

    /// Split strategy over one signal's batch; an implementation detail of [SignalDescriptor]. Only
    /// invoked for a batch strictly larger than the cap, so the singleton short-circuit is handled by
    /// [#split(Object, long)] before this is reached.
    @FunctionalInterface
    private interface Splitter<T> {
        List<T> split(T batch, long maxItems);
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

    // --- per-signal split bodies, item-count based, resource and scope grouping preserved ---

    private static List<TracesData> splitTraces(TracesData batch, long maxItems) {
        var result = new ArrayList<TracesData>();
        var current = new ArrayList<TracesData.ResourceSpans>();
        long count = 0;
        for (var rs : batch.resourceSpans()) {
            for (var ss : rs.scopeSpans()) {
                var spans = ss.spans();
                var i = 0;
                while (i < spans.size()) {
                    var take = (int) Math.min(maxItems - count, spans.size() - i);
                    var chunk = List.copyOf(spans.subList(i, i + take));
                    current.add(new TracesData.ResourceSpans(rs.resource(), rs.schemaUrl(),
                            List.of(new TracesData.ScopeSpans(ss.scope(), ss.schemaUrl(), chunk))));
                    count += take;
                    i += take;
                    if (count == maxItems) {
                        result.add(new TracesData(List.copyOf(current)));
                        current = new ArrayList<>();
                        count = 0;
                    }
                }
            }
        }
        if (!current.isEmpty()) {
            result.add(new TracesData(List.copyOf(current)));
        }
        return List.copyOf(result);
    }

    private static List<LogsData> splitLogs(LogsData batch, long maxItems) {
        var result = new ArrayList<LogsData>();
        var current = new ArrayList<LogsData.ResourceLogs>();
        long count = 0;
        for (var rl : batch.resourceLogs()) {
            for (var sl : rl.scopeLogs()) {
                var records = sl.logRecords();
                var i = 0;
                while (i < records.size()) {
                    var take = (int) Math.min(maxItems - count, records.size() - i);
                    var chunk = List.copyOf(records.subList(i, i + take));
                    current.add(new LogsData.ResourceLogs(rl.resource(), rl.schemaUrl(),
                            List.of(new LogsData.ScopeLogs(sl.scope(), sl.schemaUrl(), chunk))));
                    count += take;
                    i += take;
                    if (count == maxItems) {
                        result.add(new LogsData(List.copyOf(current)));
                        current = new ArrayList<>();
                        count = 0;
                    }
                }
            }
        }
        if (!current.isEmpty()) {
            result.add(new LogsData(List.copyOf(current)));
        }
        return List.copyOf(result);
    }

    private static List<ProfilesData> splitProfiles(ProfilesData batch, long maxItems) {
        var dictionary = batch.dictionary();
        var result = new ArrayList<ProfilesData>();
        var current = new ArrayList<ProfilesData.ResourceProfiles>();
        long count = 0;
        for (var rp : batch.resourceProfiles()) {
            for (var sp : rp.scopeProfiles()) {
                var profiles = sp.profiles();
                var i = 0;
                while (i < profiles.size()) {
                    var take = (int) Math.min(maxItems - count, profiles.size() - i);
                    var chunk = List.copyOf(profiles.subList(i, i + take));
                    current.add(new ProfilesData.ResourceProfiles(rp.resource(), rp.schemaUrl(),
                            List.of(new ProfilesData.ScopeProfiles(sp.scope(), sp.schemaUrl(), chunk))));
                    count += take;
                    i += take;
                    if (count == maxItems) {
                        result.add(new ProfilesData(List.copyOf(current), dictionary));
                        current = new ArrayList<>();
                        count = 0;
                    }
                }
            }
        }
        if (!current.isEmpty()) {
            result.add(new ProfilesData(List.copyOf(current), dictionary));
        }
        return List.copyOf(result);
    }

    private static List<MetricsData> splitMetrics(MetricsData batch, long maxItems) {
        var result = new ArrayList<MetricsData>();
        var current = new ArrayList<MetricsData.ResourceMetrics>();
        long count = 0;
        for (var rm : batch.resourceMetrics()) {
            for (var sm : rm.scopeMetrics()) {
                for (var metric : sm.metrics()) {
                    var points = metricPointCount(metric);
                    if (points == 0) {
                        current.add(wrapMetric(rm, sm, metric));
                        continue;
                    }
                    var i = 0;
                    while (i < points) {
                        var take = (int) Math.min(maxItems - count, points - i);
                        current.add(wrapMetric(rm, sm, sliceMetric(metric, i, i + take)));
                        count += take;
                        i += take;
                        if (count == maxItems) {
                            result.add(new MetricsData(List.copyOf(current)));
                            current = new ArrayList<>();
                            count = 0;
                        }
                    }
                }
            }
        }
        if (!current.isEmpty()) {
            result.add(new MetricsData(List.copyOf(current)));
        }
        return List.copyOf(result);
    }

    private static MetricsData.ResourceMetrics wrapMetric(
            MetricsData.ResourceMetrics rm, MetricsData.ScopeMetrics sm, Metric metric) {
        return new MetricsData.ResourceMetrics(rm.resource(), rm.schemaUrl(),
                List.of(new MetricsData.ScopeMetrics(sm.scope(), sm.schemaUrl(), List.of(metric))));
    }

    private static int metricPointCount(Metric metric) {
        return switch (metric.data()) {
            case Metric.NoData ignored -> 0;
            case Metric.Gauge g -> g.points().size();
            case Metric.Sum s -> s.points().size();
            case Metric.Histogram h -> h.points().size();
            case Metric.ExponentialHistogram e -> e.points().size();
            case Metric.Summary s -> s.points().size();
        };
    }

    private static Metric sliceMetric(Metric metric, int from, int to) {
        Metric.Data data = switch (metric.data()) {
            case Metric.NoData n -> n;
            case Metric.Gauge g -> new Metric.Gauge(g.points().subList(from, to));
            case Metric.Sum s -> new Metric.Sum(s.points().subList(from, to), s.temporality(), s.monotonic());
            case Metric.Histogram h -> new Metric.Histogram(h.points().subList(from, to), h.temporality());
            case Metric.ExponentialHistogram e ->
                    new Metric.ExponentialHistogram(e.points().subList(from, to), e.temporality());
            case Metric.Summary s -> new Metric.Summary(s.points().subList(from, to));
        };
        return new Metric(metric.name(), metric.description(), metric.unit(), data, metric.metadata());
    }
}
