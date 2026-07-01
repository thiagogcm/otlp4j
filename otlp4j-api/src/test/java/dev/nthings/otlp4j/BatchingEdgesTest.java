package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.LogsSink;
import dev.nthings.otlp4j.pipeline.MetricsSink;
import dev.nthings.otlp4j.processor.OverflowPolicy;
import dev.nthings.otlp4j.pipeline.ProfilesSink;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.processor.BatchingProcessor;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("BatchingProcessor edge cases")
class BatchingEdgesTest {

    @DisplayName("BLOCK drop policy eventually delivers every span")
    @Test
    void blockPolicyEventuallyDeliversAllBatches() {
        var captured = new CopyOnWriteArrayList<TracesData>();
        TracesSink downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .flushThreshold(2)
                .queueCapacity(2)
                .overflowPolicy(OverflowPolicy.BLOCK)
                .build()) {
            for (var i = 0; i < 6; i++) {
                batcher.consume(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)))
                        .toCompletableFuture().join();
            }
            batcher.forceFlush(Duration.ofSeconds(2)).toCompletableFuture().join();
        }
        var total = captured.stream().mapToInt(t -> t.spans().size()).sum();
        assertThat(total).isEqualTo(6);
        assertThat(captured).isNotEmpty();
    }

    @DisplayName("shutdown is idempotent across repeated calls")
    @Test
    void shutdownIsIdempotent() {
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .flushThreshold(10)
                .queueCapacity(10)
                .build();
        batcher.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        batcher.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
    }

    @DisplayName("queued reflects buffered items while downstream stalls")
    @Test
    void queuedReflectsBufferSize() {
        TracesSink slow = traces -> new CompletableFuture<>();
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(slow)
                .flushThreshold(100)
                .queueCapacity(10)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            batcher.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))).toCompletableFuture().join();
            assertThat(batcher.queued()).isEqualTo(2);
        }
    }

    @DisplayName("Every per-signal factory flushes at the batch-size threshold")
    @Test
    void everyPerSignalFactoryFlushesAtThreshold() {
        var traceCaptured = new AtomicReference<TracesData>();
        TracesSink traceDownstream = traces -> {
            traceCaptured.set(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var b = BatchingProcessor.forTraces().downstream(traceDownstream).flushThreshold(1)
                .queueCapacity(2).build()) {
            b.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(2)).until(() -> traceCaptured.get() != null);
        }
        var metricsCaptured = new AtomicReference<MetricsData>();
        MetricsSink metricsDownstream = m -> {
            metricsCaptured.set(m);
            return ConsumeResult.acceptedStage();
        };
        try (var b = BatchingProcessor.forMetrics().downstream(metricsDownstream).flushThreshold(1)
                .queueCapacity(2).build()) {
            b.consume(Fixtures.metricsData(Fixtures.metric("m"))).toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(2)).until(() -> metricsCaptured.get() != null);
        }
        var logsCaptured = new AtomicReference<LogsData>();
        LogsSink logsDownstream = l -> {
            logsCaptured.set(l);
            return ConsumeResult.acceptedStage();
        };
        try (var b = BatchingProcessor.forLogs().downstream(logsDownstream).flushThreshold(1)
                .queueCapacity(2).build()) {
            b.consume(Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO))).toCompletableFuture()
                    .join();
            await().atMost(Duration.ofSeconds(2)).until(() -> logsCaptured.get() != null);
        }
        var profilesCaptured = new AtomicReference<ProfilesData>();
        ProfilesSink profilesDownstream = p -> {
            profilesCaptured.set(p);
            return ConsumeResult.acceptedStage();
        };
        try (var b = BatchingProcessor.forProfilesUnsafe().downstream(profilesDownstream).flushThreshold(1)
                .queueCapacity(2).build()) {
            b.consume(Fixtures.profilesData(Fixtures.profile("p"))).toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(2)).until(() -> profilesCaptured.get() != null);
        }
    }

    @DisplayName("The periodic timer drives an age-based flush below the threshold")
    @Test
    void timerDrivesAgeBasedFlush() {
        var captured = new AtomicInteger();
        TracesSink downstream = traces -> {
            captured.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .flushThreshold(1000)
                .queueCapacity(1000)
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(3)).until(() -> captured.get() == 1);
        }
    }
}
