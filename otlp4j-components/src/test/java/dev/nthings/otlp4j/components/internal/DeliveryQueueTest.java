package dev.nthings.otlp4j.components.internal;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DeliveryQueueTest {

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("batch"), ComponentKind.PROCESSOR, SignalDescriptor.TRACES);
    private static final ConsumeContext CTX = new TestConsumeContext();
    private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

    private static DeliveryQueuePolicy policy(int queueSize, OverflowPolicy overflow, boolean waitForResult) {
        return new DeliveryQueuePolicy(queueSize, 1, overflow, waitForResult);
    }

    private static DeliveryQueue<TelemetryPayload> queue(
            DeliveryQueuePolicy policy, Duration degradedWindow, RecordingConsumer next, java.util.function.LongSupplier nano) {
        return new DeliveryQueue<>(policy, degradedWindow, next, PROV, nano);
    }

    private static boolean isDegraded(DeliveryResult result) {
        return result instanceof DeliveryResult.Rejected rejected && rejected.message().contains("degraded");
    }

    private static DeliveryResult join(java.util.concurrent.CompletionStage<DeliveryResult> stage) {
        return stage.toCompletableFuture().join();
    }

    @Test
    void rejectsNegativeDegradedWindow() {
        assertThat(new RecordingConsumer()).isNotNull();
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        new DeliveryQueue<>(policy(1, OverflowPolicy.BLOCK, false), Duration.ofSeconds(-1),
                                new RecordingConsumer(), PROV, System::nanoTime))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void waitForResultResolvesToTheDownstreamResult() {
        var next = new RecordingConsumer();
        next.result(payload -> DeliveryResult.partial(1, "one", next.provenance()));
        var q = queue(policy(8, OverflowPolicy.BLOCK, true), FIVE_SECONDS, next, System::nanoTime);
        q.start();

        var result = join(q.offer(CTX, traces(3)));

        assertThat(result).isInstanceOf(DeliveryResult.Partial.class);
        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void fireAndForgetAcceptsAtEnqueueBeforeTheDownstreamCompletes() {
        var next = new RecordingConsumer();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(8, OverflowPolicy.BLOCK, false), FIVE_SECONDS, next, System::nanoTime);
        q.start();

        var result = join(q.offer(CTX, traces(1)));
        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.completions()).isZero();

        gate.countDown();
        q.drainAndStop(FIVE_SECONDS);
        assertThat(next.completions()).isEqualTo(1);
    }

    @Test
    void failOverflowThrottlesWhenFull() {
        var next = new RecordingConsumer();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(1, OverflowPolicy.FAIL, false), FIVE_SECONDS, next, System::nanoTime);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var result = join(q.offer(CTX, traces(3)));

        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        var rejected = (DeliveryResult.Rejected) result;
        assertThat(rejected.message()).contains("full");
        assertThat(rejected.retryAfter()).isNotNull();

        gate.countDown();
        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void dropNewestDropsTheArrivingBatch() {
        var next = new RecordingConsumer();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(1, OverflowPolicy.DROP_NEWEST, false), FIVE_SECONDS, next, System::nanoTime);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var dropped = join(q.offer(CTX, traces(3)));

        assertThat(dropped).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) dropped).reason()).isEqualTo(DeliveryReason.ADMISSION_REFUSED);

        gate.countDown();
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.completions() == 2);
        assertThat(next.received().stream().map(TelemetryPayload::itemCount).toList()).containsExactly(1L, 2L);
        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void dropOldestEvictsTheHead() {
        var next = new RecordingConsumer();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(1, OverflowPolicy.DROP_OLDEST, false), FIVE_SECONDS, next, System::nanoTime);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var admitted = join(q.offer(CTX, traces(3)));

        assertThat(admitted).isInstanceOf(DeliveryResult.Accepted.class);

        gate.countDown();
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.completions() == 2);
        assertThat(next.received().stream().map(TelemetryPayload::itemCount).toList()).containsExactly(1L, 3L);
        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void dropOldestCompletesTheEvictedWaitingFuture() {
        var next = new RecordingConsumer();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(1, OverflowPolicy.DROP_OLDEST, true), FIVE_SECONDS, next, System::nanoTime);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        var evicted = q.offer(CTX, traces(2));
        q.offer(CTX, traces(3));

        var evictedResult = join(evicted);
        assertThat(evictedResult).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) evictedResult).reason()).isEqualTo(DeliveryReason.ADMISSION_REFUSED);

        gate.countDown();
        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void drainExceptionResolvesToPermanentRejection() {
        var next = new RecordingConsumer();
        next.throwOnConsume();
        var q = queue(policy(8, OverflowPolicy.BLOCK, true), FIVE_SECONDS, next, System::nanoTime);
        q.start();

        var result = join(q.offer(CTX, traces(1)));

        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) result).disposition().name()).isEqualTo("PERMANENT");
        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void degradedWindowRefusesIngressAfterAFailedDrainAndAdmitsAProbeAfterExpiry() {
        var now = new AtomicLong(1_000);
        var next = new RecordingConsumer();
        next.rejectWith(DeliveryResult.permanent("downstream down", PROV));
        var q = queue(policy(8, OverflowPolicy.BLOCK, false), Duration.ofNanos(500), next, now::get);
        q.start();

        // A failed drain trips the window; within it, ingress is refused with the degraded message.
        await().atMost(2, TimeUnit.SECONDS).until(() -> isDegraded(join(q.offer(CTX, traces(1)))));
        q.awaitDrained(FIVE_SECONDS);
        var refused = join(q.offer(CTX, traces(1)));
        assertThat(refused).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) refused).retryAfter()).isNotNull();

        // Past the window, a probe is admitted; a successful probe clears the breaker.
        now.set(2_000);
        next.result(payload -> DeliveryResult.accepted(next.provenance()));
        var probe = join(q.offer(CTX, traces(1)));
        assertThat(probe).isInstanceOf(DeliveryResult.Accepted.class);
        q.awaitDrained(FIVE_SECONDS);
        assertThat(join(q.offer(CTX, traces(1)))).isInstanceOf(DeliveryResult.Accepted.class);

        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void probeFailureReTripsTheDegradedWindow() {
        var now = new AtomicLong(1_000);
        var next = new RecordingConsumer();
        next.rejectWith(DeliveryResult.permanent("downstream down", PROV));
        var q = queue(policy(8, OverflowPolicy.BLOCK, false), Duration.ofNanos(500), next, now::get);
        q.start();

        await().atMost(2, TimeUnit.SECONDS).until(() -> isDegraded(join(q.offer(CTX, traces(1)))));
        q.awaitDrained(FIVE_SECONDS);

        // Past the window a probe is admitted, but it fails and re-trips the window from the new now.
        now.set(2_000);
        assertThat(join(q.offer(CTX, traces(1)))).isInstanceOf(DeliveryResult.Accepted.class);
        await().atMost(2, TimeUnit.SECONDS).until(() -> isDegraded(join(q.offer(CTX, traces(1)))));

        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void zeroDegradedWindowDisablesTheBreaker() {
        var next = new RecordingConsumer();
        next.rejectWith(DeliveryResult.permanent("downstream down", PROV));
        var q = queue(policy(8, OverflowPolicy.BLOCK, false), Duration.ZERO, next, System::nanoTime);
        q.start();

        join(q.offer(CTX, traces(1)));
        q.awaitDrained(FIVE_SECONDS);
        // With a zero window a rejected drain never trips the breaker, so ingress is still admitted.
        assertThat(join(q.offer(CTX, traces(1)))).isInstanceOf(DeliveryResult.Accepted.class);

        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void aSaturatedQueueTripsOverflowNotTheDegradedBreaker() {
        var next = new RecordingConsumer();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(1, OverflowPolicy.FAIL, false), Duration.ofSeconds(5), next, System::nanoTime);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        q.offer(CTX, traces(2));
        var overflow = join(q.offer(CTX, traces(3)));
        // Overflow, not degradation: the message is the full-queue message, not the degraded one.
        assertThat(isDegraded(overflow)).isFalse();
        assertThat(((DeliveryResult.Rejected) overflow).message()).contains("full");

        // The downstream was healthy all along, so after draining ingress is admitted, not broken.
        gate.countDown();
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.completions() == 2);
        assertThat(join(q.offer(CTX, traces(1)))).isInstanceOf(DeliveryResult.Accepted.class);
        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void awaitDrainedReturnsFalseWhileDeliveryIsInFlight() {
        var next = new RecordingConsumer();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(8, OverflowPolicy.BLOCK, false), FIVE_SECONDS, next, System::nanoTime);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);
        assertThat(q.awaitDrained(Duration.ofMillis(50))).isFalse();

        gate.countDown();
        assertThat(q.awaitDrained(FIVE_SECONDS)).isTrue();
        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void awaitDrainedReturnsFalseWhenTheCallerIsInterrupted() {
        var next = new RecordingConsumer();
        var gate = new CountDownLatch(1);
        next.gate(gate);
        var q = queue(policy(8, OverflowPolicy.BLOCK, false), FIVE_SECONDS, next, System::nanoTime);
        q.start();

        q.offer(CTX, traces(1));
        await().atMost(2, TimeUnit.SECONDS).until(() -> next.received().size() == 1);

        Thread.currentThread().interrupt();
        var drained = q.awaitDrained(FIVE_SECONDS);
        assertThat(Thread.interrupted()).isTrue(); // clears the flag for the rest of the suite
        assertThat(drained).isFalse();

        gate.countDown();
        q.drainAndStop(FIVE_SECONDS);
    }

    @Test
    void blockOverflowReturnsShuttingDownWhenTheEnqueueIsInterrupted() {
        var next = new RecordingConsumer();
        // No workers are started, so the single queue slot fills and stays full.
        var q = queue(policy(1, OverflowPolicy.BLOCK, false), FIVE_SECONDS, next, System::nanoTime);

        assertThat(join(q.offer(CTX, traces(1)))).isInstanceOf(DeliveryResult.Accepted.class);

        Thread.currentThread().interrupt();
        var result = join(q.offer(CTX, traces(2)));
        assertThat(Thread.interrupted()).isTrue();
        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) result).reason()).isEqualTo(DeliveryReason.SHUTDOWN);
    }

    @Test
    void offerAfterDrainAndStopIsRejected() {
        var next = new RecordingConsumer();
        var q = queue(policy(8, OverflowPolicy.BLOCK, false), FIVE_SECONDS, next, System::nanoTime);
        q.start();
        q.drainAndStop(FIVE_SECONDS);

        var result = join(q.offer(CTX, traces(1)));
        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) result).reason()).isEqualTo(DeliveryReason.SHUTDOWN);
    }
}
