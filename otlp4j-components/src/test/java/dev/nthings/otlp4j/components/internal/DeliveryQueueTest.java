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
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DeliveryQueueTest {

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("batch"), ComponentKind.PROCESSOR, SignalDescriptor.TRACES);
    private static final ConsumeContext CTX = new TestConsumeContext();
    private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

    private static DeliveryQueuePolicy policy(int queueSize, OverflowPolicy overflow) {
        return new DeliveryQueuePolicy(queueSize, 1, overflow);
    }

    private static DeliveryQueue<TracesData> queue(
            DeliveryQueuePolicy policy, Duration degradedWindow, RecordingConsumer<TracesData> next) {
        return new DeliveryQueue<>(policy, degradedWindow, next, PROV, System::nanoTime);
    }

    private static DeliveryResult join(CompletionStage<DeliveryResult> stage) {
        return stage.toCompletableFuture().join();
    }

    private static boolean isDegraded(DeliveryResult result) {
        return result instanceof DeliveryResult.Rejected rejected
                && rejected.message().contains("degraded");
    }

    @Test
    void rejectsNegativeDegradedWindow() {
        assertThatThrownBy(() -> queue(policy(1, OverflowPolicy.FAIL), Duration.ofSeconds(-1),
                new RecordingConsumer<>())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acknowledgementIsAdmissionAndFlushObservesPartialResult() {
        var next = new RecordingConsumer<TracesData>();
        next.result(payload -> DeliveryResult.partial(1, "one", next.provenance()));
        var q = queue(policy(8, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        assertThat(join(q.offer(CTX, traces(3)))).isInstanceOf(DeliveryResult.Accepted.class);
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(1);
        assertThat(report.failedBatches()).isZero();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void groupedAdmissionUsesConfiguredCapacityNotCurrentOccupancy() {
        var next = new RecordingConsumer<TracesData>();
        var q = queue(policy(2, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        var admission = q.offerAll(CTX, List.of(traces(1), traces(2)), () -> {});
        assertThat(admission.result()).isInstanceOf(DeliveryResult.Accepted.class);

        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.isSuccess()).isTrue();
        assertThat(next.totalItems()).isEqualTo(3);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void wholeDownstreamFailureCountsAllPayloadItems() {
        var next = new RecordingConsumer<TracesData>();
        next.rejectWith(DeliveryResult.permanent("downstream down", next.provenance()));
        var q = queue(policy(8, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        q.offer(CTX, traces(3));
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(1);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void refusedGroupIsIncludedInTheNextFlushReport() {
        var next = new RecordingConsumer<TracesData>();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(2, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(1));
        var refused = q.offerAll(CTX, List.of(traces(2), traces(3)), () -> {});
        assertThat(refused.result()).isInstanceOf(DeliveryResult.Rejected.class);

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(5);
        assertThat(report.failedBatches()).isEqualTo(1);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void warningOnlyPartialResultIsSuccessful() {
        var next = new RecordingConsumer<TracesData>();
        next.result(payload -> DeliveryResult.partial(0, "accepted with warning", next.provenance()));
        var q = queue(policy(8, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        q.offer(CTX, traces(1));
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.isSuccess()).isTrue();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void failOverflowRefusesWithoutEnteringPendingBarrier() {
        var next = new RecordingConsumer<TracesData>();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(1, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var refusal = join(q.offer(CTX, traces(3)));
        assertThat(refusal).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refusal).reason()).isEqualTo(DeliveryReason.LOCAL_FAILURE);
        assertThat(((DeliveryResult.Rejected) refusal).retryAfter()).isNotNull();

        gate.countDown();
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropOldestIsReportedAtFlushAndDoesNotPartiallyAdmitASplitGroup() {
        var next = new RecordingConsumer<TracesData>();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(2, OverflowPolicy.DROP_OLDEST), FIVE_SECONDS, next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        q.offer(CTX, traces(3));
        q.offerAll(CTX, List.of(traces(4), traces(5)), () -> {});

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.droppedItems()).isEqualTo(5);
        assertThat(next.totalItems()).isEqualTo(10);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void dropNewestIsReportedAsDroppedWithoutPartialAdmission() {
        var next = new RecordingConsumer<TracesData>();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(1, OverflowPolicy.DROP_NEWEST), FIVE_SECONDS, next);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var dropped = join(q.offer(CTX, traces(3)));
        assertThat(dropped).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) dropped).reason()).isEqualTo(DeliveryReason.ADMISSION_REFUSED);

        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isZero();
        assertThat(report.droppedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(1);
        assertThat(next.totalItems()).isEqualTo(3);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void fatalWorkerRejectsQueuedOwnershipAndClosesAdmission() throws InterruptedException {
        var fatal = new AssertionError("fatal downstream failure");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        SignalConsumer<TracesData> next = (context, payload) -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw fatal;
            }
            throw fatal;
        };
        var q = new DeliveryQueue<>(policy(2, OverflowPolicy.FAIL), FIVE_SECONDS, next, PROV, System::nanoTime);
        q.start();

        q.offer(CTX, traces(1));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        q.offer(CTX, traces(2));
        release.countDown();

        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();

        assertThat(report.rejectedItems()).isEqualTo(3);
        assertThat(report.failedBatches()).isEqualTo(2);
        assertThat(join(q.offer(CTX, traces(1)))).satisfies(result -> {
            assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
            assertThat(((DeliveryResult.Rejected) result).reason()).isEqualTo(DeliveryReason.SHUTDOWN);
        });
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void degradedWindowRefusesIngressThenAdmitsAProbeAfterExpiry() {
        var now = new AtomicLong(1_000);
        var next = new RecordingConsumer<TracesData>();
        next.rejectWith(DeliveryResult.permanent("downstream down", PROV));
        // The queue's time source is injectable only through construction for this deterministic
        // breaker test.
        var q = new DeliveryQueue<>(policy(8, OverflowPolicy.FAIL), Duration.ofNanos(500), next, PROV, now::get);
        q.start();

        assertThat(join(q.offer(CTX, traces(1)))).isInstanceOf(DeliveryResult.Accepted.class);
        q.flush(FIVE_SECONDS).toCompletableFuture().join();
        var refused = join(q.offer(CTX, traces(1)));
        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refused).retryAfter()).isNotNull();

        var refusalReport = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(refusalReport.rejectedItems()).isEqualTo(1);
        assertThat(refusalReport.failedBatches()).isEqualTo(1);

        now.set(2_000);
        next.result(payload -> DeliveryResult.accepted(next.provenance()));
        assertThat(join(q.offer(CTX, traces(1)))).isInstanceOf(DeliveryResult.Accepted.class);
        q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(join(q.offer(CTX, traces(1)))).isInstanceOf(DeliveryResult.Accepted.class);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void zeroDegradedWindowLeavesIngressOpenAfterDownstreamFailure() {
        var next = new RecordingConsumer<TracesData>();
        next.rejectWith(DeliveryResult.permanent("downstream down", PROV));
        var q = queue(policy(8, OverflowPolicy.FAIL), Duration.ZERO, next);
        q.start();

        q.offer(CTX, traces(1));
        q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(join(q.offer(CTX, traces(1)))).isInstanceOf(DeliveryResult.Accepted.class);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
    }

    @Test
    void flushTimeoutPreservesLateOutcomeAndShutdownRefusesNewAdmission() {
        var next = new RecordingConsumer<TracesData>();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        next.rejectWith(DeliveryResult.permanent("late downstream failure", next.provenance()));
        var q = queue(policy(8, OverflowPolicy.FAIL), FIVE_SECONDS, next);
        q.start();
        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);

        assertThatThrownBy(() -> q.flush(Duration.ofMillis(10)).toCompletableFuture().join())
                .hasRootCauseInstanceOf(TimeoutException.class);
        gate.countDown();
        var report = q.flush(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(report.rejectedItems()).isEqualTo(1);
        assertThat(report.failedBatches()).isEqualTo(1);
        q.drainAndStop(FIVE_SECONDS).toCompletableFuture().join();
        assertThat(join(q.offer(CTX, traces(1)))).satisfies(result -> {
            assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
            assertThat(((DeliveryResult.Rejected) result).reason()).isEqualTo(DeliveryReason.SHUTDOWN);
        });
    }
}
