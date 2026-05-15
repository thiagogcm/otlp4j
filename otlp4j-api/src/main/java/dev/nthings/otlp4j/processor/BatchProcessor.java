package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Buffers each signal until `maxBatchSize`, then forwards a merged batch downstream.
///
/// Buffering is synchronous and thread-safe; there is no background flush thread. A `consumeX`
/// call returns [ExportResult#success()] while buffering, or the downstream result when it flushes.
/// Call [#flush()] or [#close()] to drain telemetry below the threshold.
public final class BatchProcessor implements TelemetryConsumer, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    private final TelemetryConsumer downstream;
    private final int maxBatchSize;

    private final Object tracesLock = new Object();
    private final List<TraceData> tracesBuffer = new ArrayList<>();
    private int bufferedSpans;

    private final Object metricsLock = new Object();
    private final List<MetricsData> metricsBuffer = new ArrayList<>();
    private int bufferedMetrics;

    private final Object logsLock = new Object();
    private final List<LogsData> logsBuffer = new ArrayList<>();
    private int bufferedLogRecords;

    private final Object profilesLock = new Object();
    private final List<ProfilesData> profilesBuffer = new ArrayList<>();
    private int bufferedProfiles;

    private BatchProcessor(TelemetryConsumer downstream, int maxBatchSize) {
        if (downstream == null) {
            throw new IllegalArgumentException("downstream must be set");
        }
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException("maxBatchSize must be >= 1");
        }
        this.downstream = downstream;
        this.maxBatchSize = maxBatchSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ExportResult consumeTraces(TraceData traces) {
        synchronized (tracesLock) {
            tracesBuffer.add(traces);
            bufferedSpans += traces.spans().size();
            return bufferedSpans >= maxBatchSize ? flushTraces() : ExportResult.success();
        }
    }

    @Override
    public ExportResult consumeMetrics(MetricsData metrics) {
        synchronized (metricsLock) {
            metricsBuffer.add(metrics);
            bufferedMetrics += metrics.metrics().size();
            return bufferedMetrics >= maxBatchSize ? flushMetrics() : ExportResult.success();
        }
    }

    @Override
    public ExportResult consumeLogs(LogsData logs) {
        synchronized (logsLock) {
            logsBuffer.add(logs);
            bufferedLogRecords += logs.logRecords().size();
            return bufferedLogRecords >= maxBatchSize ? flushLogs() : ExportResult.success();
        }
    }

    @Override
    public ExportResult consumeProfiles(ProfilesData profiles) {
        synchronized (profilesLock) {
            profilesBuffer.add(profiles);
            bufferedProfiles += profiles.profiles().size();
            return bufferedProfiles >= maxBatchSize ? flushProfiles() : ExportResult.success();
        }
    }

    /// Forwards every buffered signal downstream immediately, returning the combined result.
    public ExportResult flush() {
        var result = ExportResult.success();
        synchronized (tracesLock) {
            result = result.and(flushTraces());
        }
        synchronized (metricsLock) {
            result = result.and(flushMetrics());
        }
        synchronized (logsLock) {
            result = result.and(flushLogs());
        }
        synchronized (profilesLock) {
            result = result.and(flushProfiles());
        }
        return result;
    }

    /// Drains any buffered telemetry downstream.
    @Override
    public void close() {
        flush();
    }

    private ExportResult flushTraces() {
        if (tracesBuffer.isEmpty()) {
            return ExportResult.success();
        }
        var merged = new ArrayList<TraceData.ResourceSpans>();
        tracesBuffer.forEach(traces -> merged.addAll(traces.resourceSpans()));
        log.debug("flushing {} buffered spans downstream", bufferedSpans);
        tracesBuffer.clear();
        bufferedSpans = 0;
        return downstream.consumeTraces(new TraceData(merged));
    }

    private ExportResult flushMetrics() {
        if (metricsBuffer.isEmpty()) {
            return ExportResult.success();
        }
        var merged = new ArrayList<MetricsData.ResourceMetrics>();
        metricsBuffer.forEach(metrics -> merged.addAll(metrics.resourceMetrics()));
        log.debug("flushing {} buffered metrics downstream", bufferedMetrics);
        metricsBuffer.clear();
        bufferedMetrics = 0;
        return downstream.consumeMetrics(new MetricsData(merged));
    }

    private ExportResult flushLogs() {
        if (logsBuffer.isEmpty()) {
            return ExportResult.success();
        }
        var merged = new ArrayList<LogsData.ResourceLogs>();
        logsBuffer.forEach(logs -> merged.addAll(logs.resourceLogs()));
        log.debug("flushing {} buffered log records downstream", bufferedLogRecords);
        logsBuffer.clear();
        bufferedLogRecords = 0;
        return downstream.consumeLogs(new LogsData(merged));
    }

    private ExportResult flushProfiles() {
        if (profilesBuffer.isEmpty()) {
            return ExportResult.success();
        }
        var merged = new ArrayList<ProfilesData.ResourceProfiles>();
        profilesBuffer.forEach(profiles -> merged.addAll(profiles.resourceProfiles()));
        log.debug("flushing {} buffered profiles downstream", bufferedProfiles);
        profilesBuffer.clear();
        bufferedProfiles = 0;
        return downstream.consumeProfiles(new ProfilesData(merged));
    }

    public static final class Builder {

        private TelemetryConsumer downstream;
        private int maxBatchSize = 512;

        private Builder() {}

        /// The consumer that receives the coalesced batches. Required.
        public Builder downstream(TelemetryConsumer downstream) {
            this.downstream = downstream;
            return this;
        }

        /// Flush threshold: a signal is forwarded once this many items are buffered.
        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public BatchProcessor build() {
            return new BatchProcessor(downstream, maxBatchSize);
        }
    }
}
