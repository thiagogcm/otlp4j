package dev.nthings.otlp4j.components.internal;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DeliveryQueueFailureTest {

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("batch"), ComponentKind.PROCESSOR, SignalDescriptor.TRACES);
    private static final ConsumeContext CTX = new TestConsumeContext();
    private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

    private static DeliveryQueuePolicy policy(int queueSize, OverflowPolicy overflow) {
        return new DeliveryQueuePolicy(queueSize, 1, overflow);
    }

    private static <T> DeliveryQueue<T> queue(
            DeliveryQueuePolicy policy, Duration degradedWindow, SignalConsumer<T> next) {
        return new DeliveryQueue<>(policy, degradedWindow, next, PROV, System::nanoTime);
    }

    private static void assertFlushTimeout(CompletionStage<FlushReport> stage) {
        assertThatThrownBy(() -> stage.toCompletableFuture().join())
                .hasRootCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void failOverflowRejectsAnEntireGroupThatExceedsCapacity() {
        var next = new RecordingConsumer<TracesData>();
        var completions = new AtomicInteger();
        var q = queue(policy(1, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        var admission = q.offerAll(
                CTX, List.of(traces(1), traces(2)), completions::incrementAndGet);

        assertThat(admission.result()).isInstanceOf(DeliveryResult.Rejected.class);
        var rejected = (DeliveryResult.Rejected) admission.result();
        assertThat(rejected.reason()).isEqualTo(DeliveryReason.LOCAL_FAILURE);
        assertThat(rejected.retryAfter()).isNotNull();
        assertThat(completions).hasValue(0);
        assertThat(next.received()).isEmpty();

        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(report.droppedItems()).isZero();
        assertThat(report.failedBatches()).isEqualTo(1);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropNewestReportsAnEntireGroupThatExceedsCapacityAsDropped() {
        var next = new RecordingConsumer<TracesData>();
        var completions = new AtomicInteger();
        var q = queue(policy(1, OverflowPolicy.DROP_NEWEST), FIVE_SECONDS, next);
        q.start();

        var admission = q.offerAll(
                CTX, List.of(traces(1), traces(2)), completions::incrementAndGet);

        assertThat(admission.result()).isInstanceOf(DeliveryResult.Rejected.class);
        var rejected = (DeliveryResult.Rejected) admission.result();
        assertThat(rejected.reason()).isEqualTo(DeliveryReason.ADMISSION_REFUSED);
        assertThat(rejected.retryAfter()).isNull();
        assertThat(completions).hasValue(0);

        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isZero();
        assertThat(report.droppedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(1);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void nullDownstreamResultBecomesARecordedWholeBatchFailure() {
        SignalConsumer<TracesData> next = (context, payload) -> CompletableFuture.completedFuture(null);
        var q = queue(policy(2, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        assertThat(q.offer(CTX, traces(2)).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(2);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(report.firstFailure())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("null delivery result");
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void repeatedTimeoutsCoalesceCarryAndPreserveBothLateFailures() {
        var next = new RecordingConsumer<TracesData>();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        next.rejectWith(DeliveryResult.permanent("late failure", next.provenance()));
        var q = queue(policy(8, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertFlushTimeout(q.flush(Duration.ofMillis(10)));

        q.offer(CTX, traces(2));
        assertFlushTimeout(q.flush(Duration.ofMillis(10)));

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(report.droppedItems()).isZero();
        assertThat(report.failedBatches()).isEqualTo(2);
        assertThat(report.firstFailure()).isNotNull();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void fatalDownstreamErrorRunsTheGroupedCompletionAndReleasesItsEntry() {
        var fatal = new AssertionError("fatal downstream failure");
        SignalConsumer<TracesData> next = (context, payload) -> {
            throw fatal;
        };
        var completions = new AtomicInteger();
        var q = queue(policy(2, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        var admission = q.offerAll(CTX, List.of(traces(1)), completions::incrementAndGet);
        assertThat(admission.result()).isInstanceOf(DeliveryResult.Accepted.class);
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(completions).hasValue(1);
        assertThat(report.rejectedItems()).isEqualTo(1);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(report.firstFailure()).isSameAs(fatal);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropOldestSeparatesEvictionsFromDownstreamPartialRejections() {
        var next = new RecordingConsumer<TracesData>();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        next.result(payload -> DeliveryResult.partial(1, "partial", next.provenance()));
        var q = queue(policy(2, OverflowPolicy.DROP_OLDEST), FIVE_SECONDS, next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        q.offer(CTX, traces(3));
        q.offer(CTX, traces(4));

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(report.droppedItems()).isEqualTo(2);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(next.totalItems()).isEqualTo(8);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropNewestCombinesAGroupRejectionWithPartialDownstreamResults() {
        var next = new RecordingConsumer<TracesData>();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        next.result(payload -> DeliveryResult.partial(1, "partial", next.provenance()));
        var q = queue(policy(1, OverflowPolicy.DROP_NEWEST), FIVE_SECONDS, next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var dropped = q.offer(CTX, traces(3)).toCompletableFuture().join();
        assertThat(dropped).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) dropped).reason())
                .isEqualTo(DeliveryReason.ADMISSION_REFUSED);

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(2);
        assertThat(report.droppedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(next.totalItems()).isEqualTo(3);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void shutdownTimeoutLeavesLateOutcomeAvailableThroughTheNextFlush() throws InterruptedException {
        var delivered = new CompletableFuture<DeliveryResult>();
        var received = new CountDownLatch(1);
        SignalConsumer<TracesData> next = (context, payload) -> {
            received.countDown();
            return delivered;
        };
        var q = queue(policy(2, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();
        q.offer(CTX, traces(1));
        try {
            assertThat(received.await(2, TimeUnit.SECONDS)).isTrue();
            assertFlushTimeout(q.drainAndStop(Duration.ofMillis(20)));
        } finally {
            delivered.complete(DeliveryResult.accepted(PROV));
        }

        var lateReport = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(lateReport.isSuccess()).isTrue();
    }
}
