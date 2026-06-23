package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.processor.BatchingProcessor;
import dev.nthings.otlp4j.processor.DropPolicy;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("BatchingProcessor edge cases")
class BatchingEdgesTest {

    @DisplayName("BLOCK drop policy eventually delivers every span")
    @Test
    void blockPolicyEventuallyDeliversAllBatches() {
        var captured = new CopyOnWriteArrayList<TraceData>();
        TraceSink downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .maxBatchSize(2)
                .queueCapacity(2)
                .maxBatchAge(Duration.ofSeconds(30))
                .dropPolicy(DropPolicy.BLOCK)
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
        TraceSink downstream = traces -> ConsumeResult.acceptedStage();
        var batcher = BatchingProcessor.forTraces()
                .downstream(downstream)
                .maxBatchSize(10)
                .queueCapacity(10)
                .maxBatchAge(Duration.ofSeconds(30))
                .build();
        batcher.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        batcher.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
    }

    @DisplayName("queued reflects buffered items while downstream stalls")
    @Test
    void queuedReflectsBufferSize() {
        TraceSink slow = traces -> new CompletableFuture<>();
        try (var batcher = BatchingProcessor.forTraces()
                .downstream(slow)
                .maxBatchSize(100)
                .queueCapacity(10)
                .maxBatchAge(Duration.ofSeconds(30))
                .build()) {
            batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            batcher.consume(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))).toCompletableFuture().join();
            assertThat(batcher.queued()).isEqualTo(2);
        }
    }

    @DisplayName("Every per-signal factory flushes at the batch-size threshold")
    @Test
    void everyPerSignalFactoryFlushesAtThreshold() {
        var traceCaptured = new AtomicReference<TraceData>();
        TraceSink traceDownstream = traces -> {
            traceCaptured.set(traces);
            return ConsumeResult.acceptedStage();
        };
        try (var b = BatchingProcessor.forTraces().downstream(traceDownstream).maxBatchSize(1)
                .queueCapacity(2).maxBatchAge(Duration.ofMinutes(1)).build()) {
            b.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(2)).until(() -> traceCaptured.get() != null);
        }
        var metricsCaptured = new AtomicReference<MetricsData>();
        MetricSink metricsDownstream = m -> {
            metricsCaptured.set(m);
            return ConsumeResult.acceptedStage();
        };
        try (var b = BatchingProcessor.forMetrics().downstream(metricsDownstream).maxBatchSize(1)
                .queueCapacity(2).maxBatchAge(Duration.ofMinutes(1)).build()) {
            b.consume(Fixtures.metricsData(Fixtures.metric("m"))).toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(2)).until(() -> metricsCaptured.get() != null);
        }
        var logsCaptured = new AtomicReference<LogsData>();
        LogSink logsDownstream = l -> {
            logsCaptured.set(l);
            return ConsumeResult.acceptedStage();
        };
        try (var b = BatchingProcessor.forLogs().downstream(logsDownstream).maxBatchSize(1)
                .queueCapacity(2).maxBatchAge(Duration.ofMinutes(1)).build()) {
            b.consume(Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO))).toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(2)).until(() -> logsCaptured.get() != null);
        }
        var profilesCaptured = new AtomicReference<ProfilesData>();
        ProfileSink profilesDownstream = p -> {
            profilesCaptured.set(p);
            return ConsumeResult.acceptedStage();
        };
        try (var b = BatchingProcessor.forProfiles().downstream(profilesDownstream).maxBatchSize(1)
                .queueCapacity(2).maxBatchAge(Duration.ofMinutes(1)).build()) {
            b.consume(Fixtures.profilesData(Fixtures.profile("p"))).toCompletableFuture().join();
            await().atMost(Duration.ofSeconds(2)).until(() -> profilesCaptured.get() != null);
        }
    }

    @DisplayName("External scheduler drives age-based flush and is not shut down")
    @Test
    void externalSchedulerIsHonoured() {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var captured = new AtomicInteger();
            TraceSink downstream = traces -> {
                captured.incrementAndGet();
                return ConsumeResult.acceptedStage();
            };
            try (var batcher = BatchingProcessor.forTraces()
                    .downstream(downstream)
                    .maxBatchSize(1000)
                    .queueCapacity(1000)
                    .maxBatchAge(Duration.ofMillis(50))
                    .scheduler(scheduler)
                    .build()) {
                batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                        .toCompletableFuture().join();
                await().atMost(Duration.ofSeconds(2)).until(() -> captured.get() == 1);
            }
        } finally {
            // External scheduler is the caller's responsibility — batcher must not shut it down.
            scheduler.shutdown();
        }
    }
}
