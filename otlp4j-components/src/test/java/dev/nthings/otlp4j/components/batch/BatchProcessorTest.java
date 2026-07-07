package dev.nthings.otlp4j.components.batch;

import static dev.nthings.otlp4j.components.support.BatchSupport.baseContext;
import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static dev.nthings.otlp4j.components.support.BatchSupport.tracesBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class BatchProcessorTest {

    private static final Duration NO_TIMER = Duration.ofHours(1);
    private static final ShutdownContext SHUTDOWN = new ShutdownContext(Duration.ofSeconds(5));
    private static final ConsumeContext CTX = new TestConsumeContext();

    private static BatchProcessorConfig config(
            int target, int maxSize, long maxBytes, Duration timeout, List<String> keys,
            int queueSize, int consumers, OverflowPolicy overflow, boolean waitForResult) {
        return new BatchProcessorConfig(
                new BatchPolicy(target, maxSize, maxBytes, timeout, keys),
                new DeliveryQueuePolicy(queueSize, consumers, overflow, waitForResult),
                Duration.ofSeconds(5));
    }

    private static BatchProcessorConfig sizeTriggered(int target) {
        return config(target, 0, 0, NO_TIMER, List.of(), 64, 1, OverflowPolicy.BLOCK, false);
    }

    private static Processor<TelemetryPayload> start(BatchProcessorConfig config, RecordingConsumer next) {
        var processor = tracesBatch("batch", config, next);
        processor.start(baseContext()).toCompletableFuture().join();
        return processor;
    }

    @Test
    void flushesOnItemCountTrigger() {
        var next = new RecordingConsumer();
        var processor = start(sizeTriggered(3), next);

        assertAccepted(processor.consume(CTX, traces(1)));
        assertAccepted(processor.consume(CTX, traces(1)));
        assertThat(next.received()).isEmpty();
        assertAccepted(processor.consume(CTX, traces(1)));

        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertThat(next.received().get(0).itemCount()).isEqualTo(3);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void flushesOnByteSizeTrigger() {
        // A huge item target, but a one-byte cap forces a flush on the first payload's serialized size.
        var next = new RecordingConsumer();
        var processor = start(
                config(1_000_000, 0, 1, NO_TIMER, List.of(), 64, 1, OverflowPolicy.BLOCK, false), next);

        assertAccepted(processor.consume(CTX, traces(2)));

        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertThat(next.received().get(0).itemCount()).isEqualTo(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void flushesOnTimeout() {
        var next = new RecordingConsumer();
        var processor = start(
                config(1_000_000, 0, 0, Duration.ofMillis(100), List.of(), 64, 1, OverflowPolicy.BLOCK, false),
                next);

        assertAccepted(processor.consume(CTX, traces(2)));
        assertThat(next.received()).isEmpty();

        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertThat(next.received().get(0).itemCount()).isEqualTo(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void splitsAnOverLargeMergedBatchToTheSendBatchMaxSize() {
        var next = new RecordingConsumer();
        // Target 2 flushes on the single 4-span payload; a max size of 2 splits the merged batch in two.
        var processor = start(
                config(2, 2, 0, NO_TIMER, List.of(), 64, 1, OverflowPolicy.BLOCK, false), next);

        assertAccepted(processor.consume(CTX, traces(4)));

        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 2);
        assertThat(next.received()).allSatisfy(p -> assertThat(p.itemCount()).isEqualTo(2));
        assertThat(next.totalItems()).isEqualTo(4);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void partitionsBatchesByMetadataKey() {
        var next = new RecordingConsumer();
        var processor = start(config(2, 0, 0, NO_TIMER, List.of("tenant"), 64, 1, OverflowPolicy.BLOCK, false), next);

        processor.consume(TestConsumeContext.withHeader("tenant", "a"), traces(1));
        processor.consume(TestConsumeContext.withHeader("tenant", "b"), traces(1));
        processor.consume(TestConsumeContext.withHeader("tenant", "a"), traces(1));

        // Only tenant a reaches the target of two; tenant b stays buffered.
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertThat(next.received().get(0).itemCount()).isEqualTo(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void unsetAndEmptyMetadataAreDistinctPartitions() {
        var next = new RecordingConsumer();
        var processor = start(config(2, 0, 0, NO_TIMER, List.of("tenant"), 64, 1, OverflowPolicy.BLOCK, false), next);

        var unset = new TestConsumeContext();
        var empty = TestConsumeContext.withHeader("tenant");

        processor.consume(unset, traces(1));
        processor.consume(empty, traces(1));

        // If unset and present-but-empty collapsed to one key, the batch would reach two and flush.
        // They are distinct, so neither reaches the target; a forced flush yields two separate batches.
        processor.forceFlush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();
        assertThat(next.received()).hasSize(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void waitForResultReturnsTheRealDownstreamResult() throws Exception {
        var next = new RecordingConsumer();
        next.result(payload -> DeliveryResult.partial(2, "half rejected", next.provenance()));
        var processor = start(config(1, 0, 0, NO_TIMER, List.of(), 64, 1, OverflowPolicy.BLOCK, true), next);

        var result = processor.consume(CTX, traces(5)).toCompletableFuture().get(2, TimeUnit.SECONDS);

        assertThat(result).isInstanceOf(DeliveryResult.Partial.class);
        assertThat(((DeliveryResult.Partial) result).rejectedItems()).isEqualTo(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void fireAndForgetAcksAtEnqueueEvenWhenTheDownstreamRejects() {
        var next = new RecordingConsumer();
        next.rejectWith(DeliveryResult.permanent("downstream down", next.provenance()));
        var processor = start(config(1, 0, 0, NO_TIMER, List.of(), 64, 1, OverflowPolicy.BLOCK, false), next);

        assertAccepted(processor.consume(CTX, traces(1)));
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void forceFlushDrainsBufferedBatches() {
        var next = new RecordingConsumer();
        var processor = start(sizeTriggered(1_000_000), next);

        processor.consume(CTX, traces(2));
        processor.consume(CTX, traces(3));
        assertThat(next.received()).isEmpty();

        processor.forceFlush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();

        assertThat(next.totalItems()).isEqualTo(5);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void shutdownFlushesBufferedBatchesIntoTheStillOpenDownstream() {
        var next = new RecordingConsumer();
        var processor = start(sizeTriggered(1_000_000), next);

        processor.consume(CTX, traces(2));
        processor.consume(CTX, traces(2));

        processor.shutdown(SHUTDOWN).toCompletableFuture().join();

        assertThat(next.totalItems()).isEqualTo(4);
    }

    @Test
    void shutdownIsIdempotent() {
        var next = new RecordingConsumer();
        var processor = start(sizeTriggered(1_000_000), next);

        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void consumeAfterShutdownIsRefused() {
        var next = new RecordingConsumer();
        var processor = start(sizeTriggered(1_000_000), next);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();

        var result = processor.consume(CTX, traces(1)).toCompletableFuture().join();
        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
    }

    @Test
    void nonMutatingCapability() {
        var processor = tracesBatch("batch", sizeTriggered(1), new RecordingConsumer());
        assertThat(processor.capabilities().mutatesData()).isFalse();
    }

    @Test
    void metadataKeyedContextCarriesHeaders() {
        // A sanity check that the metadata context is wired the way the batcher reads it.
        var ctx = new TestConsumeContext(new ClientMetadata(java.util.Optional.empty(),
                java.util.Map.of("tenant", List.of("x"))));
        assertThat(ctx.metadata().headers()).containsEntry("tenant", List.of("x"));
    }

    private static void assertAccepted(java.util.concurrent.CompletionStage<DeliveryResult> stage) {
        try {
            var result = stage.toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError("stage did not complete with accepted", e);
        }
    }
}
