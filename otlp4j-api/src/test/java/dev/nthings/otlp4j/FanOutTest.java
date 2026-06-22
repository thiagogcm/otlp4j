package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.FanOut;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("FanOut")
class FanOutTest {

    @DisplayName("consume() delivers the batch to every peer consumer")
    @Test
    void deliversToEveryPeer() {
        var hits = new AtomicInteger();
        TraceConsumer peer = traces -> {
            hits.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        var fan = FanOut.<TraceData>of(peer, peer, peer);
        var result = fan.consume(new TraceData(List.of())).toCompletableFuture().join();
        assertThat(hits.get()).isEqualTo(3);
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("One failing peer still produces a merged Rejected result")
    @Test
    void onePeerFailureDoesNotBlockOthers() {
        TraceConsumer healthy = traces -> ConsumeResult.acceptedStage();
        TraceConsumer broken = traces -> CompletableFuture.failedStage(new RuntimeException("nope"));
        var fan = FanOut.<TraceData>of(broken, healthy);
        var result = fan.consume(new TraceData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("of() rejects an empty peer set")
    @Test
    void rejectsEmptyPeerSet() {
        assertThatThrownBy(() -> FanOut.<TraceData>of(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(FanOut::of)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("consume() merges peer Partial counts using the max")
    @Test
    void mergesRejectionCountsWithMax() {
        TraceConsumer p1 = traces -> CompletableFuture.completedStage(ConsumeResult.partial(3L, "one"));
        TraceConsumer p2 = traces -> CompletableFuture.completedStage(ConsumeResult.partial(7L, "two"));
        var fan = FanOut.<TraceData>of(p1, p2);
        var result = fan.consume(new TraceData(List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Partial.class);
        assertThat(((ConsumeResult.Partial<TraceData>) result).rejectedItems()).isEqualTo(7L);
    }
}
