package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.FanOut;
import dev.nthings.otlp4j.core.TraceSink;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("FanOut")
class FanOutTest {

    @DisplayName("consume() delivers the batch to every peer consumer")
    @Test
    void deliversToEveryPeer() {
        var hits = new AtomicInteger();
        TraceSink peer = traces -> {
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
        TraceSink healthy = traces -> ConsumeResult.acceptedStage();
        TraceSink broken = traces -> CompletableFuture.failedStage(new RuntimeException("nope"));
        var fan = FanOut.<TracesData>of(broken, healthy);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("of() rejects an empty peer set")
    @Test
    void rejectsEmptyPeerSet() {
        assertThatThrownBy(() -> FanOut.<TracesData>of(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(FanOut::of)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("Error-throwing peer is caught and does not block others")
    @Test
    void captureErrorThrowingPeer() {
        TraceSink healthy = traces -> ConsumeResult.acceptedStage();
        TraceSink broken = traces -> { throw new AssertionError("boom"); };
        var fan = FanOut.<TracesData>of(broken, healthy);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("Rejection message includes exception class when message is null")
    @Test
    void rejectionMessageIncludesExceptionClass() {
        TraceSink broken = traces -> { throw new IllegalStateException(); };
        var fan = FanOut.<TracesData>of(broken);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                r -> assertThat(r.message()).contains("java.lang.IllegalStateException"));
    }

    @DisplayName("Rejection message unwraps CompletionException to show the cause")
    @Test
    void rejectionMessageUnwrapsCompletionException() {
        TraceSink broken = traces -> CompletableFuture.failedFuture(
                new CompletionException(new IllegalArgumentException("cause")));
        var fan = FanOut.<TracesData>of(broken);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                r -> assertThat(r.message()).contains("cause"));
    }

    @DisplayName("consume() merges peer Partial counts using the max")
    @Test
    void mergesRejectionCountsWithMax() {
        TraceSink p1 = traces -> CompletableFuture.completedStage(ConsumeResult.partial(3L, "one"));
        TraceSink p2 = traces -> CompletableFuture.completedStage(ConsumeResult.partial(7L, "two"));
        var fan = FanOut.<TracesData>of(p1, p2);
        var result = fan.consume(new TracesData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Partial.class);
        assertThat(((ConsumeResult.Partial<TracesData>) result).rejectedItems()).isEqualTo(7L);
    }
}
