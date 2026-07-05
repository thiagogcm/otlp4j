package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.ops.AckMode;
import dev.nthings.otlp4j.pipeline.ops.Batcher;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Exercises [Batcher.Builder#circuitBreaker]: sustained downstream failure
/// surfaces at ingress as retryable rejections with a retry-after hint, and clears on recovery.
@Timeout(30)
@DisplayName("Batcher downstream-failure reflection")
class BatchingFeedbackTest {

    private static TracesData batch() {
        return Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
    }

    /// Downstream whose health a test flips at will.
    private static final class SwitchableDownstream implements TracesSink {

        final AtomicBoolean healthy = new AtomicBoolean(false);

        @Override
        public java.util.concurrent.CompletionStage<ConsumeResult> consume(TracesData batch) {
            return healthy.get()
                    ? ConsumeResult.acceptedStage()
                    : java.util.concurrent.CompletableFuture.completedFuture(
                            ConsumeResult.retryable("backend down"));
        }
    }

    @DisplayName("off (default): a failing downstream never rejects at ingress")
    @Test
    void defaultKeepsFireAndForget() {
        var downstream = new SwitchableDownstream();
        // ON_ENQUEUE: this asserts the fire-and-forget contract (ingress always accepts against an
        // unhealthy downstream). Under ON_DELIVERY the first ack would carry the real Rejected.
        var batcher = Batcher.forTraces().downstream(downstream).ackMode(AckMode.ON_ENQUEUE).build();
        try {
            assertThat(batcher.consume(batch()).toCompletableFuture().join())
                    .isInstanceOf(ConsumeResult.Accepted.class);
            assertThatThrownBy(() -> batcher.forceFlush(Duration.ofSeconds(5)).toCompletableFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(Batcher.BatchDeliveryException.class);

            // Even right after the failed flush, ingress still accepts.
            assertThat(batcher.consume(batch()).toCompletableFuture().join())
                    .isInstanceOf(ConsumeResult.Accepted.class);
        } finally {
            batcher.close();
        }
    }

    @DisplayName("on: a failed flush degrades ingress - throttled rejection, batch not enqueued")
    @Test
    void failedFlushDegradesIngress() {
        var downstream = new SwitchableDownstream();
        var batcher = Batcher.forTraces()
                .downstream(downstream)
                // ON_ENQUEUE: the first consume must be accepted before the failed flush degrades
                // ingress; under ON_DELIVERY its ack would carry the downstream Rejected instead.
                .ackMode(AckMode.ON_ENQUEUE)
                .circuitBreaker(true)
                .build();
        try {
            assertThat(batcher.consume(batch()).toCompletableFuture().join())
                    .isInstanceOf(ConsumeResult.Accepted.class);
            assertThatThrownBy(() -> batcher.forceFlush(Duration.ofSeconds(5)).toCompletableFuture().join())
                    .isInstanceOf(CompletionException.class);

            var result = batcher.consume(batch()).toCompletableFuture().join();
            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, r -> {
                assertThat(r.retryable()).isTrue();
                assertThat(r.retryAfter()).isEqualTo(Duration.ofSeconds(1));
                assertThat(r.message()).contains("degraded");
                assertThat(r.cause()).isNotNull();
            });
            // The rejected batch was never enqueued: nothing new is queued.
            assertThat(batcher.queued()).isZero();
        } finally {
            batcher.close();
        }
    }

    @DisplayName("on: the window expires to admit a probe, and a successful drain clears the state")
    @Test
    void windowExpiresAndSuccessClears() throws Exception {
        var downstream = new SwitchableDownstream();
        var batcher = Batcher.forTraces()
                .downstream(downstream)
                // ON_ENQUEUE: this races an explicit forceFlush against the age timer and asserts
                // enqueue acceptance before degrade; under ON_DELIVERY the consume join would drain
                // on the timer first and the forceFlush would find an empty queue.
                .ackMode(AckMode.ON_ENQUEUE)
                .circuitBreaker(true)
                .build();
        try {
            batcher.consume(batch()).toCompletableFuture().join();
            assertThatThrownBy(() -> batcher.forceFlush(Duration.ofSeconds(5)).toCompletableFuture().join())
                    .isInstanceOf(CompletionException.class);
            assertThat(batcher.consume(batch()).toCompletableFuture().join())
                    .isInstanceOf(ConsumeResult.Rejected.class);

            // Heal the downstream and let the degraded window lapse: the next batch is admitted
            // as a probe.
            downstream.healthy.set(true);
            Thread.sleep(1200);
            assertThat(batcher.consume(batch()).toCompletableFuture().join())
                    .isInstanceOf(ConsumeResult.Accepted.class);

            // A successful drain clears the degraded state entirely.
            batcher.forceFlush(Duration.ofSeconds(5)).toCompletableFuture().join();
            assertThat(batcher.consume(batch()).toCompletableFuture().join())
                    .isInstanceOf(ConsumeResult.Accepted.class);
        } finally {
            batcher.close();
        }
    }

    @DisplayName("ON_DELIVERY composes with reflection: the in-flight ack learns the rejection, the next batch is rejected at ingress")
    @Test
    void onDeliveryComposesWithReflection() {
        var downstream = new SwitchableDownstream();   // starts unhealthy
        var batcher = Batcher.forTraces()
                .downstream(downstream)
                .circuitBreaker(true)
                .flushThreshold(100)   // drain only on the explicit forceFlush
                .build();   // ON_DELIVERY default
        try {
            var inFlight = batcher.consume(batch()).toCompletableFuture();   // queued, ack pending
            // The failed drain surfaces on forceFlush and also opens the degraded window.
            assertThatThrownBy(() -> batcher.forceFlush(Duration.ofSeconds(5)).toCompletableFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(Batcher.BatchDeliveryException.class);

            // ON_DELIVERY (reactive): the in-flight caller learns the real downstream rejection.
            assertThat(inFlight.join()).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    r -> assertThat(r.message()).contains("backend down"));

            // circuitBreaker (proactive): the next batch is rejected at ingress, unqueued.
            assertThat(batcher.consume(batch()).toCompletableFuture().join())
                    .isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                            r -> assertThat(r.message()).contains("degraded"));
            assertThat(batcher.queued()).isZero();
        } finally {
            batcher.close();
        }
    }
}
