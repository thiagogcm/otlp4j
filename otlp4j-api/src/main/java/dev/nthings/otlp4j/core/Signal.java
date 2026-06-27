package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.processor.BatchMergers;
import java.util.List;
import java.util.function.ToLongFunction;

/// Identifies one of the four OTLP signals and binds its batch type, item
/// counter, and merge strategy. Used internally to remove four-way repetition
/// across batching, dispatch, and tests.
public enum Signal {

    TRACES(TraceData.class, BatchMergers::mergeTraces, TraceData::spanCount),
    METRICS(MetricsData.class, BatchMergers::mergeMetrics, MetricsData::dataPointCount),
    LOGS(LogsData.class, BatchMergers::mergeLogs, LogsData::logRecordCount),
    PROFILES(ProfilesData.class, BatchMergers::mergeProfilesUnsafe, ProfilesData::profileCount);

    private final Class<?> batchType;
    private final Merger<?> merger;
    private final ToLongFunction<?> itemCounter;

    <T> Signal(Class<T> batchType, Merger<T> merger, ToLongFunction<T> itemCounter) {
        this.batchType = batchType;
        this.merger = merger;
        this.itemCounter = itemCounter;
    }

    /// The domain batch type for this signal.
    public Class<?> batchType() {
        return batchType;
    }

    /// Merges queued batches of this signal into one downstream batch.
    @SuppressWarnings("unchecked")
    public <T> T merge(List<T> snapshot) {
        Merger<T> typed = (Merger<T>) merger;
        return typed.merge(snapshot);
    }

    /// Counts OTLP items in one batch for partial-success reporting.
    @SuppressWarnings("unchecked")
    public <T> long itemCount(T batch) {
        return ((ToLongFunction<T>) itemCounter).applyAsLong(batch);
    }

    /// Wraps a batch in the matching [Telemetry] envelope.
    public Telemetry envelope(Object batch) {
        return switch (this) {
            case TRACES -> new Telemetry.Traces((TraceData) batch);
            case METRICS -> new Telemetry.Metrics((MetricsData) batch);
            case LOGS -> new Telemetry.Logs((LogsData) batch);
            case PROFILES -> new Telemetry.Profiles((ProfilesData) batch);
        };
    }

    @FunctionalInterface
    interface Merger<T> {
        T merge(List<T> snapshot);
    }
}
