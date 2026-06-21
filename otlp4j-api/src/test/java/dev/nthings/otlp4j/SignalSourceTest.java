package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.receiver.internal.SignalSource;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("SignalSource")
class SignalSourceTest {

    @DisplayName("dispatch returns Accepted when no consumer is attached")
    @Test
    void dispatchWithoutConsumerReturnsAccepted() {
        var source = new SignalSource<>(TraceData.class);
        var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("dispatch invokes the attached consumer once per batch")
    @Test
    void dispatchInvokesAttachedConsumer() {
        var source = new SignalSource<>(TraceData.class);
        var hits = new AtomicInteger();
        TraceConsumer c = traces -> {
            hits.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        var sub = source.subscribe(c);
        try {
            source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            source.dispatch(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))).toCompletableFuture().join();
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        assertThat(hits.get()).isEqualTo(2);
    }

    @DisplayName("subscribe rejects a second concurrent subscription")
    @Test
    void doubleSubscribeRejected() {
        var source = new SignalSource<>(TraceData.class);
        var sub = source.subscribe(traces -> ConsumeResult.acceptedStage());
        try {
            assertThatThrownBy(() -> source.subscribe(traces -> ConsumeResult.acceptedStage()))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("dispatch propagates exceptions thrown by the consumer")
    @Test
    void dispatchPropagatesConsumerException() {
        var source = new SignalSource<>(TraceData.class);
        TraceConsumer c = traces -> { throw new RuntimeException("boom"); };
        var sub = source.subscribe(c);
        try {
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                    source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("boom");
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }
}
