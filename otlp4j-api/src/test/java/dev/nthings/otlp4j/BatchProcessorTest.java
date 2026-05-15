package dev.nthings.otlp4j;

import static dev.nthings.otlp4j.testing.Fixtures.logRecord;
import static dev.nthings.otlp4j.testing.Fixtures.logsData;
import static dev.nthings.otlp4j.testing.Fixtures.metric;
import static dev.nthings.otlp4j.testing.Fixtures.metricsData;
import static dev.nthings.otlp4j.testing.Fixtures.profile;
import static dev.nthings.otlp4j.testing.Fixtures.profilesData;
import static dev.nthings.otlp4j.testing.Fixtures.span;
import static dev.nthings.otlp4j.testing.Fixtures.traceData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import dev.nthings.otlp4j.processor.BatchProcessor;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Unit and concurrency tests for `BatchProcessor`.
///
/// Concurrency assertions focus on deterministic no-loss invariants, not timing-dependent flush
/// counts.
@Timeout(30)
class BatchProcessorTest {

    @Test
    void coalescesUntilThresholdThenFlushes() {
        var batches = new AtomicInteger();
        var spans = new AtomicInteger();
        assertCoalescesUntilThreshold(counting(batches, spans), batches, spans,
                (batch, name) -> batch.consumeTraces(traceData(span(name, Span.Kind.INTERNAL))));
    }

    @Test
    void coalescesMetricsUntilThresholdThenFlushes() {
        var batches = new AtomicInteger();
        var metrics = new AtomicInteger();
        assertCoalescesUntilThreshold(countingMetrics(batches, metrics), batches, metrics,
                (batch, name) -> batch.consumeMetrics(metricsData(metric(name))));
    }

    @Test
    void coalescesLogsUntilThresholdThenFlushes() {
        var batches = new AtomicInteger();
        var logs = new AtomicInteger();
        assertCoalescesUntilThreshold(countingLogs(batches, logs), batches, logs,
                (batch, name) -> batch.consumeLogs(logsData(logRecord(name, LogRecord.Severity.INFO))));
    }

    @Test
    void coalescesProfilesUntilThresholdThenFlushes() {
        var batches = new AtomicInteger();
        var profiles = new AtomicInteger();
        assertCoalescesUntilThreshold(countingProfiles(batches, profiles), batches, profiles,
                (batch, name) -> batch.consumeProfiles(profilesData(profile(name))));
    }

    @Test
    void flushDrainsEveryBufferedSignalInOneCall() {
        var traces = new AtomicInteger();
        var metrics = new AtomicInteger();
        var logs = new AtomicInteger();
        var profiles = new AtomicInteger();
        var sink = new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData t) {
                traces.addAndGet(t.spans().size());
                return ExportResult.success();
            }

            @Override
            public ExportResult consumeMetrics(MetricsData m) {
                metrics.addAndGet(m.metrics().size());
                return ExportResult.partialSuccess(1, "one metric rejected");
            }

            @Override
            public ExportResult consumeLogs(LogsData l) {
                logs.addAndGet(l.logRecords().size());
                return ExportResult.success();
            }

            @Override
            public ExportResult consumeProfiles(ProfilesData p) {
                profiles.addAndGet(p.profiles().size());
                return ExportResult.success();
            }
        };
        ExportResult result;
        try (var batch =
                BatchProcessor.builder().downstream(sink).maxBatchSize(100).build()) {
            batch.consumeTraces(traceData(span("a", Span.Kind.INTERNAL)));
            batch.consumeMetrics(metricsData(metric("m")));
            batch.consumeLogs(logsData(logRecord("l", LogRecord.Severity.INFO)));
            batch.consumeProfiles(profilesData(profile("p")));

            result = batch.flush();
        }
        assertThat(traces.get()).isEqualTo(1);
        assertThat(metrics.get()).isEqualTo(1);
        assertThat(logs.get()).isEqualTo(1);
        assertThat(profiles.get()).isEqualTo(1);
        assertThat(result.rejectedCount())
                .as("flush() combines every signal's downstream result")
                .isEqualTo(1);
        assertThat(result.message()).isEqualTo("one metric rejected");
    }

    @Test
    void flushOnEmptyBuffersReturnsFullSuccess() {
        try (var batch = BatchProcessor.builder()
                .downstream(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeTraces(TraceData traces) {
                        throw new AssertionError("downstream must not be touched for empty buffers");
                    }
                })
                .maxBatchSize(10)
                .build()) {
            assertThat(batch.flush().isFullSuccess())
                    .as("flush() with nothing buffered hits the isEmpty() early-return in each flushX")
                    .isTrue();
        }
    }

    @Test
    void oversizedSingleBatchIsForwardedWhole() {
        var spansForwarded = new AtomicInteger();
        var batchSizes = new ArrayList<Integer>();
        var sink = new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData traces) {
                batchSizes.add(traces.spans().size());
                spansForwarded.addAndGet(traces.spans().size());
                return ExportResult.success();
            }
        };
        try (var batch =
                BatchProcessor.builder().downstream(sink).maxBatchSize(3).build()) {
            batch.consumeTraces(traceData(
                    span("a", Span.Kind.INTERNAL),
                    span("b", Span.Kind.INTERNAL),
                    span("c", Span.Kind.INTERNAL),
                    span("d", Span.Kind.INTERNAL),
                    span("e", Span.Kind.INTERNAL)));
        }
        assertThat(batchSizes)
                .as("a single consumeTraces above maxBatchSize is flushed whole, not split")
                .containsExactly(5);
        assertThat(spansForwarded.get()).isEqualTo(5);
    }

    @Test
    void builderRejectsAnUnsetDownstream() {
        assertThatThrownBy(() -> BatchProcessor.builder().build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("downstream must be set");
    }

    @Test
    void builderRejectsANonPositiveBatchSize() {
        assertThatThrownBy(() -> BatchProcessor.builder()
                        .downstream(new TelemetryConsumer() {})
                        .maxBatchSize(0)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchSize must be >= 1");
    }

    @Test
    void isThreadSafeUnderConcurrentExport() throws Exception {
        var forwarded = new AtomicInteger();
        assertNoLossUnderContention(countingSpans(forwarded), forwarded,
                batch -> batch.consumeTraces(traceData(span("s", Span.Kind.INTERNAL))));
    }

    @Test
    void isThreadSafeUnderConcurrentMetricsExport() throws Exception {
        var forwarded = new AtomicInteger();
        assertNoLossUnderContention(countingMetrics(new AtomicInteger(), forwarded), forwarded,
                batch -> batch.consumeMetrics(metricsData(metric("m"))));
    }

    @Test
    void isThreadSafeUnderConcurrentLogsExport() throws Exception {
        var forwarded = new AtomicInteger();
        assertNoLossUnderContention(countingLogs(new AtomicInteger(), forwarded), forwarded,
                batch -> batch.consumeLogs(logsData(logRecord("l", LogRecord.Severity.INFO))));
    }

    @Test
    void isThreadSafeUnderConcurrentProfilesExport() throws Exception {
        var forwarded = new AtomicInteger();
        assertNoLossUnderContention(countingProfiles(new AtomicInteger(), forwarded), forwarded,
                batch -> batch.consumeProfiles(profilesData(profile("p"))));
    }

    @Test
    void perSignalLocksAreIndependentUnderConcurrentMultiSignalExport() throws Exception {
        var threadsPerSignal = 4;
        var exportsPerThread = 100;
        var spans = new AtomicInteger();
        var metrics = new AtomicInteger();
        var logs = new AtomicInteger();
        var profiles = new AtomicInteger();
        var sink = new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData t) {
                spans.addAndGet(t.spans().size());
                return ExportResult.success();
            }

            @Override
            public ExportResult consumeMetrics(MetricsData m) {
                metrics.addAndGet(m.metrics().size());
                return ExportResult.success();
            }

            @Override
            public ExportResult consumeLogs(LogsData l) {
                logs.addAndGet(l.logRecords().size());
                return ExportResult.success();
            }

            @Override
            public ExportResult consumeProfiles(ProfilesData p) {
                profiles.addAndGet(p.profiles().size());
                return ExportResult.success();
            }
        };

        var pool = Executors.newFixedThreadPool(threadsPerSignal * 4);
        try (var batch =
                BatchProcessor.builder().downstream(sink).maxBatchSize(16).build()) {
            var start = new CountDownLatch(1);
            var futures = new ArrayList<Future<?>>();
            for (var t = 0; t < threadsPerSignal; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (var i = 0; i < exportsPerThread; i++) {
                        batch.consumeTraces(traceData(span("s", Span.Kind.INTERNAL)));
                    }
                    return null;
                }));
                futures.add(pool.submit(() -> {
                    start.await();
                    for (var i = 0; i < exportsPerThread; i++) {
                        batch.consumeMetrics(metricsData(metric("m")));
                    }
                    return null;
                }));
                futures.add(pool.submit(() -> {
                    start.await();
                    for (var i = 0; i < exportsPerThread; i++) {
                        batch.consumeLogs(logsData(logRecord("l", LogRecord.Severity.INFO)));
                    }
                    return null;
                }));
                futures.add(pool.submit(() -> {
                    start.await();
                    for (var i = 0; i < exportsPerThread; i++) {
                        batch.consumeProfiles(profilesData(profile("p")));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        var expected = threadsPerSignal * exportsPerThread;
        assertThat(spans.get())
                .as("traces uncontaminated by concurrent metrics/logs/profiles traffic")
                .isEqualTo(expected);
        assertThat(metrics.get()).as("metrics uncontaminated").isEqualTo(expected);
        assertThat(logs.get()).as("logs uncontaminated").isEqualTo(expected);
        assertThat(profiles.get()).as("profiles uncontaminated").isEqualTo(expected);
    }

    /// Drives one signal through the buffer-below-threshold / flush-at-threshold / drain-on-close
    /// lifecycle. `exportOne` exports a single item named after the given string.
    private static void assertCoalescesUntilThreshold(
            TelemetryConsumer downstream,
            AtomicInteger batches,
            AtomicInteger items,
            BiConsumer<BatchProcessor, String> exportOne) {
        try (var batch =
                BatchProcessor.builder().downstream(downstream).maxBatchSize(3).build()) {
            exportOne.accept(batch, "a");
            exportOne.accept(batch, "b");
            assertThat(batches.get()).as("still buffering below threshold").isZero();

            exportOne.accept(batch, "c");
            assertThat(batches.get()).as("threshold reached -> one flush").isEqualTo(1);
            assertThat(items.get()).isEqualTo(3);

            exportOne.accept(batch, "d");
        }
        assertThat(batches.get()).as("close() drained the remainder").isEqualTo(2);
        assertThat(items.get()).isEqualTo(4);
    }

    /// Hammers one signal from eight threads and asserts no telemetry is lost under contention —
    /// `close()` flushes the remainder. `exportOnce` performs a single export on the batch.
    private static void assertNoLossUnderContention(
            TelemetryConsumer downstream, AtomicInteger forwarded, Consumer<BatchProcessor> exportOnce)
            throws Exception {
        var threads = 8;
        var exportsPerThread = 100;
        var pool = Executors.newFixedThreadPool(threads);
        try (var batch = BatchProcessor.builder()
                .downstream(downstream)
                .maxBatchSize(16)
                .build()) {
            var start = new CountDownLatch(1);
            var futures = new ArrayList<Future<?>>();
            for (var t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (var i = 0; i < exportsPerThread; i++) {
                        exportOnce.accept(batch);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(forwarded.get())
                .as("no telemetry lost under contention")
                .isEqualTo(threads * exportsPerThread);
    }

    private static TelemetryConsumer counting(AtomicInteger batches, AtomicInteger spans) {
        return new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData traces) {
                batches.incrementAndGet();
                spans.addAndGet(traces.spans().size());
                return ExportResult.success();
            }
        };
    }

    private static TelemetryConsumer countingSpans(AtomicInteger spans) {
        return counting(new AtomicInteger(), spans);
    }

    private static TelemetryConsumer countingMetrics(AtomicInteger batches, AtomicInteger metrics) {
        return new TelemetryConsumer() {
            @Override
            public ExportResult consumeMetrics(MetricsData m) {
                batches.incrementAndGet();
                metrics.addAndGet(m.metrics().size());
                return ExportResult.success();
            }
        };
    }

    private static TelemetryConsumer countingLogs(AtomicInteger batches, AtomicInteger logs) {
        return new TelemetryConsumer() {
            @Override
            public ExportResult consumeLogs(LogsData l) {
                batches.incrementAndGet();
                logs.addAndGet(l.logRecords().size());
                return ExportResult.success();
            }
        };
    }

    private static TelemetryConsumer countingProfiles(
            AtomicInteger batches, AtomicInteger profiles) {
        return new TelemetryConsumer() {
            @Override
            public ExportResult consumeProfiles(ProfilesData p) {
                batches.incrementAndGet();
                profiles.addAndGet(p.profiles().size());
                return ExportResult.success();
            }
        };
    }
}
