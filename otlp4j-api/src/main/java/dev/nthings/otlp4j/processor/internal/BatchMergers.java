package dev.nthings.otlp4j.processor.internal;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/// Signal-specific merge strategies for `BatchingProcessor`.
public final class BatchMergers {

    private BatchMergers() {
    }

    public static TracesData mergeTraces(List<TracesData> snapshot) {
        var combined = new ArrayList<TracesData.ResourceSpans>();
        snapshot.forEach(t -> combined.addAll(t.resourceSpans()));
        return new TracesData(combined);
    }

    public static MetricsData mergeMetrics(List<MetricsData> snapshot) {
        var combined = new ArrayList<MetricsData.ResourceMetrics>();
        snapshot.forEach(m -> combined.addAll(m.resourceMetrics()));
        return new MetricsData(combined);
    }

    public static LogsData mergeLogs(List<LogsData> snapshot) {
        var combined = new ArrayList<LogsData.ResourceLogs>();
        snapshot.forEach(l -> combined.addAll(l.resourceLogs()));
        return new LogsData(combined);
    }

    /// Profiles carry an opaque, batch-level `ProfilesDictionary`; merging is only
    /// lossless when the batches agree on that dictionary. Distinct non-empty
    /// dictionaries cannot be merged without re-indexing every reference, so this
    /// fails loudly rather than emitting a corrupted batch.
    ///
    /// Prefer forwarding profiles 1:1. Use only through
    /// `BatchingProcessor.forProfilesUnsafe()`.
    public static ProfilesData mergeProfilesUnsafe(List<ProfilesData> snapshot) {
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
