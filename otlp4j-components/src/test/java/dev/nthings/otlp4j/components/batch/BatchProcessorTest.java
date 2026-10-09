package dev.nthings.otlp4j.components.batch;

import static dev.nthings.otlp4j.components.support.BatchSupport.baseContext;
import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static dev.nthings.otlp4j.components.support.BatchSupport.tracesBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class BatchProcessorTest {

    private static final Duration NO_TIMER = Duration.ZERO;
    private static final ShutdownContext SHUTDOWN = new ShutdownContext(Duration.ofSeconds(5));
    private static final ConsumeContext CTX = new TestConsumeContext();

    /// A sizer that weighs every payload at one hundred bytes per item, so byte limits are exact.
    private static final PayloadSizer PER_ITEM = payload -> 100L * payload.itemCount();

    private static BatchProcessorConfig config(
            int target, int maxSize, long maxBytes, Duration timeout, List<String> keys) {
        return config(target, maxSize, maxBytes, timeout, keys, 64, 1,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_SUBMISSIONS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES,
                BatchProcessorConfig.DEFAULT_MAX_PARTITIONS);
    }

    private static BatchProcessorConfig config(
            int target, int maxSize, long maxBytes, Duration timeout, List<String> keys,
            int queueSize, int consumers, int maxPending, long maxPendingBytes, int maxPartitions) {
        return new BatchProcessorConfig(
                new BatchPolicy(target, maxSize, maxBytes, timeout, keys),
                new DeliveryQueuePolicy(queueSize, consumers, OverflowPolicy.FAIL),
                Duration.ofSeconds(5), maxPending, maxPendingBytes, maxPartitions,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_ITEMS, PER_ITEM);
    }

    private static BatchProcessorConfig sizeTriggered(int target) {
        return config(target, 0, 0, NO_TIMER, List.of());
    }

    private static Processor<TracesData> start(
            BatchProcessorConfig config, RecordingConsumer<TracesData> next) {
        var processor = tracesBatch("batch", config, next);
        processor.start(baseContext()).toCompletableFuture().join();
        return processor;
    }

    @Test
    void flushesOnItemCountTrigger() {
        var next = new RecordingConsumer<TracesData>();
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
    void flushesOnExactByteSizeTrigger() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(1_000_000, 0, 200, NO_TIMER, List.of()), next);

        assertAccepted(processor.consume(CTX, traces(2)));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertThat(next.received().get(0).itemCount()).isEqualTo(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void rejectsAnIndivisibleItemBeforeAdmissionWhenByteCapIsTooSmall() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(1_000_000, 0, 99, NO_TIMER, List.of()), next);

        var refused = processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refused).disposition())
                .isEqualTo(Disposition.PERMANENT);
        assertThat(next.received()).isEmpty();
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void rejectsPayloadsThatCannotBeMaterialised() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(sizeTriggered(1_000_000), next);
        var valid = traces(1);
        Payload<TracesData> broken = new Payload<>() {
            @Override
            public SignalDescriptor<TracesData> signal() {
                return SignalDescriptor.TRACES;
            }

            @Override
            public TracesData data() {
                throw new IllegalStateException("cannot decode payload");
            }

            @Override
            public long itemCount() {
                return valid.itemCount();
            }

            @Override
            public RoutingMetadata routingMetadata() {
                return valid.routingMetadata();
            }

            @Override
            public Payload<TracesData> withData(TracesData replacement) {
                return valid.withData(replacement);
            }
        };

        var refused = processor.consume(CTX, broken).toCompletableFuture().join();

        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refused).disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(next.received()).isEmpty();
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void checksCancellationAndDeadlineBeforeMaterialising() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(sizeTriggered(1_000_000), next);
        var expired = new ConsumeContext() {
            @Override
            public ClientMetadata metadata() {
                return ClientMetadata.empty();
            }

            @Override
            public Deadline deadline() {
                return new Deadline(Instant.EPOCH);
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public void onCancel(Runnable callback) {
                // no-op
            }
        };

        var refused = processor.consume(expired, new Payload<TracesData>() {
            @Override public SignalDescriptor<TracesData> signal() { return SignalDescriptor.TRACES; }
            @Override public TracesData data() { throw new AssertionError("must not decode a refused request"); }
            @Override public long itemCount() { return 1; }
            @Override public RoutingMetadata routingMetadata() { return traces(1).routingMetadata(); }
            @Override public Payload<TracesData> withData(TracesData data) { return Payload.of(SignalDescriptor.TRACES, data); }
        }).toCompletableFuture().join();

        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refused).reason())
                .isEqualTo(DeliveryReason.TIMEOUT);
        assertThat(next.received()).isEmpty();
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void checksCancellationBeforeMaterialising() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(sizeTriggered(1_000_000), next);
        var cancelled = new ConsumeContext() {
            @Override
            public ClientMetadata metadata() {
                return ClientMetadata.empty();
            }

            @Override
            public Deadline deadline() {
                return Deadline.never();
            }

            @Override
            public boolean isCancelled() {
                return true;
            }

            @Override
            public void onCancel(Runnable callback) {
                // no-op
            }
        };

        var refused = processor.consume(cancelled, new Payload<TracesData>() {
            @Override public SignalDescriptor<TracesData> signal() { return SignalDescriptor.TRACES; }
            @Override public TracesData data() { throw new AssertionError("must not decode a refused request"); }
            @Override public long itemCount() { return 1; }
            @Override public RoutingMetadata routingMetadata() { return traces(1).routingMetadata(); }
            @Override public Payload<TracesData> withData(TracesData data) { return Payload.of(SignalDescriptor.TRACES, data); }
        }).toCompletableFuture().join();

        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refused).disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(next.received()).isEmpty();
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void contextCaptureFailureDoesNotReserveAdmissionCapacity() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(1, 0, 0, NO_TIMER, List.of(), 64, 1, 1,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES, 1), next);
        var broken = new ConsumeContext() {
            @Override
            public ClientMetadata metadata() {
                throw new IllegalStateException("metadata unavailable");
            }

            @Override
            public Deadline deadline() {
                return Deadline.never();
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public void onCancel(Runnable callback) {
                // no-op
            }
        };

        var refused = processor.consume(broken, traces(1)).toCompletableFuture().join();
        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertAccepted(processor.consume(CTX, traces(1)));
        processor.flush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();
        assertThat(next.totalItems()).isEqualTo(1);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void detachesAcceptedContextBeforeDownstreamDelivery() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(sizeTriggered(1), next);

        assertAccepted(processor.consume(CTX, traces(1)));
        processor.flush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();

        assertThat(next.contexts()).singleElement().satisfies(context -> {
            assertThat(context.deadline().at()).isEqualTo(Instant.MAX);
            assertThat(context.isCancelled()).isFalse();
        });
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void flushesOnTimeout() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(1_000_000, 0, 0, Duration.ofMillis(100), List.of()), next);

        assertAccepted(processor.consume(CTX, traces(2)));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertThat(next.received().get(0).itemCount()).isEqualTo(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void splitsAnOverLargeMergedBatch() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(2, 2, 0, NO_TIMER, List.of()), next);

        assertAccepted(processor.consume(CTX, traces(4)));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 2);
        assertThat(next.received()).allSatisfy(p -> assertThat(p.itemCount()).isEqualTo(2));
        assertThat(next.totalItems()).isEqualTo(4);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void partitionsByMetadata() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(2, 0, 0, NO_TIMER, List.of("tenant")), next);

        assertAccepted(processor.consume(TestConsumeContext.withHeader("tenant", "a"), traces(1)));
        assertAccepted(processor.consume(TestConsumeContext.withHeader("tenant", "b"), traces(1)));
        assertAccepted(processor.consume(TestConsumeContext.withHeader("tenant", "a"), traces(1)));

        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertThat(next.received().get(0).itemCount()).isEqualTo(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void unsetAndEmptyMetadataAreDistinctPartitions() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(2, 0, 0, NO_TIMER, List.of("tenant")), next);

        assertAccepted(processor.consume(new TestConsumeContext(), traces(1)));
        assertAccepted(processor.consume(TestConsumeContext.withHeader("tenant"), traces(1)));
        var report = processor.flush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();

        assertThat(report.isSuccess()).isTrue();
        assertThat(next.received()).hasSize(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void flushReportsPartialDownstreamOutcomeAfterAdmissionAck() {
        var next = new RecordingConsumer<TracesData>();
        next.result(payload -> DeliveryResult.partial(2, "half rejected", next.provenance()));
        var processor = start(config(1, 0, 0, NO_TIMER, List.of()), next);

        assertAccepted(processor.consume(CTX, traces(5)));
        var report = processor.flush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void globalPendingSubmissionLimitRefusesBeforeBuffering() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(1_000_000, 0, 0, NO_TIMER, List.of(),
                64, 1, 1, BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES, 1024), next);

        assertAccepted(processor.consume(CTX, traces(1)));
        var refused = processor.consume(CTX, traces(1)).toCompletableFuture().join();
        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(next.received()).isEmpty();

        processor.flush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void globalPendingByteLimitRefusesBeforeBuffering() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(1_000_000, 0, 0, NO_TIMER, List.of(),
                64, 1, 1024, 99, 1024), next);

        var refused = processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refused).reason()).isEqualTo(DeliveryReason.ADMISSION_REFUSED);
        assertThat(next.received()).isEmpty();
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void unweighedSubmissionsAreChargedByItemsAndTheBudgetReturnsAfterDelivery() {
        var next = new RecordingConsumer<TracesData>();
        // The default sizer cannot weigh a payload built in process, so the one-byte limit never
        // applies and the three-item limit does.
        var processor = start(new BatchProcessorConfig(
                new BatchPolicy(1_000_000, 0, 0, NO_TIMER, List.of()),
                new DeliveryQueuePolicy(64, 1, OverflowPolicy.FAIL),
                Duration.ofSeconds(5), 1024, 1, 1024, 3, PayloadSizer.reported()), next);

        assertAccepted(processor.consume(CTX, traces(2)));
        var refused = processor.consume(CTX, traces(2)).toCompletableFuture().join();
        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refused).reason()).isEqualTo(DeliveryReason.ADMISSION_REFUSED);
        assertAccepted(processor.consume(CTX, traces(1)));

        processor.flush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();
        assertThat(next.totalItems()).isEqualTo(3);
        assertAccepted(processor.consume(CTX, traces(3)));
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void anUnweighedSubmissionAboveTheWholeItemLimitIsAdmittedOnlyWhenNothingUnweighedIsPending() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(new BatchProcessorConfig(
                new BatchPolicy(1_000_000, 0, 0, NO_TIMER, List.of()),
                new DeliveryQueuePolicy(64, 1, OverflowPolicy.FAIL),
                Duration.ofSeconds(5), 1024, 1, 1024, 3, PayloadSizer.reported()), next);

        // Five items exceed the three-item limit, yet refusing them forever would lose every such batch.
        assertAccepted(processor.consume(CTX, traces(5)));
        assertThat(processor.consume(CTX, traces(1)).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Rejected.class);
        // The item limit is now exceeded, which must not refuse a payload charged in bytes.
        assertAccepted(processor.consume(CTX, encoded(traces(1), 1)));

        processor.flush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();
        assertThat(next.totalItems()).isEqualTo(6);
        assertAccepted(processor.consume(CTX, traces(5)));
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void aSizerThatCannotWeighUnderAByteCapOrReturnsANegativeWeightIsAPermanentRejection() {
        var next = new RecordingConsumer<TracesData>();
        var delivery = new DeliveryQueuePolicy(64, 1, OverflowPolicy.FAIL);
        PayloadSizer unable = payload -> 0;
        PayloadSizer negative = payload -> -1;
        var capped = start(new BatchProcessorConfig(
                new BatchPolicy(1_000_000, 0, 1024, NO_TIMER, List.of()), delivery,
                Duration.ofSeconds(5), 1024, 1024, 1024, 1024, unable), next);
        var broken = start(new BatchProcessorConfig(
                new BatchPolicy(1_000_000, 0, 0, NO_TIMER, List.of()), delivery,
                Duration.ofSeconds(5), 1024, 1024, 1024, 1024, negative), next);

        // Letting either through would export a request the byte cap or the byte limit never saw.
        for (var processor : List.of(capped, broken)) {
            var refused = processor.consume(CTX, traces(2)).toCompletableFuture().join();
            assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
            assertThat(((DeliveryResult.Rejected) refused).disposition()).isEqualTo(Disposition.PERMANENT);
            processor.shutdown(SHUTDOWN).toCompletableFuture().join();
        }
        assertThat(next.received()).isEmpty();
    }

    @Test
    void aPayloadThatCarriesItsEncodedSizeIsChargedInBytesAndNotByItems() {
        var next = new RecordingConsumer<TracesData>();
        // One pending item would refuse a three-item payload if it were charged by items.
        var processor = start(new BatchProcessorConfig(
                new BatchPolicy(1_000_000, 0, 0, NO_TIMER, List.of()),
                new DeliveryQueuePolicy(64, 1, OverflowPolicy.FAIL),
                Duration.ofSeconds(5), 1024, 900, 1024, 1, PayloadSizer.reported()), next);

        assertAccepted(processor.consume(CTX, encoded(traces(3), 500)));
        var refused = processor.consume(CTX, encoded(traces(1), 500)).toCompletableFuture().join();

        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refused).reason()).isEqualTo(DeliveryReason.ADMISSION_REFUSED);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void globalPartitionLimitRefusesNewPartition() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(config(1_000_000, 0, 0, NO_TIMER, List.of("tenant"),
                64, 1, 1024, BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES, 1), next);

        assertAccepted(processor.consume(TestConsumeContext.withHeader("tenant", "a"), traces(1)));
        var refused = processor.consume(TestConsumeContext.withHeader("tenant", "b"), traces(1))
                .toCompletableFuture().join();
        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void automaticQueueRefusalDoesNotWithdrawTheAdmissionAcknowledgement() {
        var next = new RecordingConsumer<TracesData>();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var processor = start(config(1, 0, 0, NO_TIMER, List.of(),
                1, 1, 1024, BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES, 1024), next);

        assertAccepted(processor.consume(CTX, traces(1)));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertAccepted(processor.consume(CTX, traces(1)));
        assertAccepted(processor.consume(CTX, traces(1)));

        gate.countDown();
        var report = processor.flush(new FlushContext(Duration.ofSeconds(2))).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(1);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(next.totalItems()).isEqualTo(2);
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
    }

    @Test
    void shutdownFlushesBufferedBatchesAndIsIdempotent() {
        var next = new RecordingConsumer<TracesData>();
        var processor = start(sizeTriggered(1_000_000), next);
        assertAccepted(processor.consume(CTX, traces(2)));
        assertAccepted(processor.consume(CTX, traces(2)));

        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
        assertThat(next.totalItems()).isEqualTo(4);
    }

    @Test
    void consumeAfterShutdownIsRefused() {
        var processor = start(sizeTriggered(1_000_000), new RecordingConsumer<>());
        processor.shutdown(SHUTDOWN).toCompletableFuture().join();
        assertThat(processor.consume(CTX, traces(1)).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Rejected.class);
    }

    @Test
    void repeatedShutdownPreservesTheFailure() {
        var next = new RecordingConsumer<TracesData>();
        next.rejectWith(DeliveryResult.permanent("downstream down", next.provenance()));
        var processor = start(sizeTriggered(1), next);
        assertAccepted(processor.consume(CTX, traces(1)));

        var first = processor.shutdown(SHUTDOWN);
        assertThatThrownBy(() -> processor.shutdown(SHUTDOWN).toCompletableFuture().join())
                .hasRootCauseMessage("downstream down");
        assertThatThrownBy(() -> first.toCompletableFuture().join())
                .hasRootCauseMessage("downstream down");
    }

    /// A payload that reports `bytes` as its encoded size, as one received over OTLP does.
    private static Payload<TracesData> encoded(Payload<TracesData> payload, long bytes) {
        return new Payload<>() {
            @Override public SignalDescriptor<TracesData> signal() { return payload.signal(); }
            @Override public TracesData data() { return payload.data(); }
            @Override public long itemCount() { return payload.itemCount(); }
            @Override public RoutingMetadata routingMetadata() { return payload.routingMetadata(); }
            @Override public OptionalLong encodedSize() { return OptionalLong.of(bytes); }
            @Override public Payload<TracesData> withData(TracesData replacement) {
                return payload.withData(replacement);
            }
        };
    }

    private static void assertAccepted(CompletionStage<DeliveryResult> stage) {
        try {
            assertThat(stage.toCompletableFuture().get(2, TimeUnit.SECONDS))
                    .isInstanceOf(DeliveryResult.Accepted.class);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError("stage did not complete with accepted", e);
        }
    }
}
