package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.FanOut;
import dev.nthings.otlp4j.pipeline.TracesSink;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("FanOut")
class FanOutTest {

    @DisplayName("consume() delivers the batch to every peer consumer")
    @Test
    void deliversToEveryPeer() {
        var hits = new AtomicInteger();
        TracesSink peer = traces -> {
            hits.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        var fan = FanOut.<TracesData>of(peer, peer, peer);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(hits.get()).isEqualTo(3);
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("One failing peer still produces a merged Rejected result")
    @Test
    void onePeerFailureDoesNotBlockOthers() {
        TracesSink healthy = traces -> ConsumeResult.acceptedStage();
        TracesSink broken = traces -> CompletableFuture.failedStage(new RuntimeException("nope"));
        var fan = FanOut.<TracesData>of(broken, healthy);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("of() with no peers accepts and discards every batch")
    @Test
    void emptyFanOutAcceptsAndDiscards() {
        var empty = FanOut.<TracesData>of(List.of());
        var result = empty.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);

        var emptyVarargs = FanOut.<TracesData>of();
        assertThat(emptyVarargs.consume(new TracesData(List.of())).toCompletableFuture().join())
                .isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("of() with a single peer returns the peer itself")
    @Test
    void singlePeerCollapsesToPeerIdentity() {
        TracesSink peer = traces -> ConsumeResult.acceptedStage();
        assertThat(FanOut.<TracesData>of(peer)).isSameAs(peer);
        assertThat(FanOut.<TracesData>of(List.of(peer))).isSameAs(peer);
    }

    @DisplayName("Error-throwing peer is caught and does not block others")
    @Test
    void captureErrorThrowingPeer() {
        TracesSink healthy = traces -> ConsumeResult.acceptedStage();
        TracesSink broken = traces -> { throw new AssertionError("boom"); };
        var fan = FanOut.<TracesData>of(broken, healthy);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("Rejection message includes exception class when message is null")
    @Test
    void rejectionMessageIncludesExceptionClass() {
        TracesSink healthy = traces -> ConsumeResult.acceptedStage();
        TracesSink broken = traces -> { throw new IllegalStateException(); };
        var fan = FanOut.<TracesData>of(broken, healthy);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                r -> assertThat(r.message()).contains("java.lang.IllegalStateException"));
    }

    @DisplayName("Rejection message unwraps CompletionException to show the cause")
    @Test
    void rejectionMessageUnwrapsCompletionException() {
        TracesSink healthy = traces -> ConsumeResult.acceptedStage();
        TracesSink broken = traces -> CompletableFuture.failedFuture(
                new CompletionException(new IllegalArgumentException("cause")));
        var fan = FanOut.<TracesData>of(broken, healthy);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                r -> assertThat(r.message()).contains("cause"));
    }

    @DisplayName("peers are launched concurrently, not one after another on the caller thread")
    @Test
    void peersLaunchConcurrently() {
        // Each peer trips the shared latch and then waits for it to reach zero. The wait can only
        // clear when BOTH peers have started, so a sequential caller-thread dispatch (peer 0 blocks
        // before peer 1 is ever launched) would deadlock; genuine concurrent launch completes.
        var bothStarted = new CountDownLatch(2);
        TracesSink gated = traces -> {
            bothStarted.countDown();
            try {
                if (!bothStarted.await(5, TimeUnit.SECONDS)) {
                    return CompletableFuture.completedStage(ConsumeResult.permanent("peer never saw its sibling start"));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CompletableFuture.completedStage(ConsumeResult.permanent("interrupted", e));
            }
            return ConsumeResult.acceptedStage();
        };
        var fan = FanOut.<TracesData>of(gated, gated);
        var result = fan.consume(new TracesData(List.of()))
                .toCompletableFuture().orTimeout(10, TimeUnit.SECONDS).join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(bothStarted.getCount()).isZero();
    }

    @DisplayName("consume() merges peer Partial counts using the max")
    @Test
    void mergesRejectionCountsWithMax() {
        TracesSink p1 = traces -> CompletableFuture.completedStage(ConsumeResult.partial(3L, "one"));
        TracesSink p2 = traces -> CompletableFuture.completedStage(ConsumeResult.partial(7L, "two"));
        var fan = FanOut.<TracesData>of(p1, p2);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Partial.class);
        assertThat(((ConsumeResult.Partial) result).rejectedItems()).isEqualTo(7L);
    }
}
