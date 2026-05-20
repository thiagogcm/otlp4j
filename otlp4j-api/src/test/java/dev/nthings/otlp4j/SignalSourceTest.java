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
import org.junit.jupiter.api.Test;

class SignalSourceTest {

    @Test
    void dispatchWithoutConsumerReturnsAccepted() {
        var source = new SignalSource<>(TraceData.class);
        var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @Test
    void dispatchInvokesAttachedConsumer() {
        var source = new SignalSource<>(TraceData.class);
        var hits = new AtomicInteger();
        TraceConsumer c = traces -> {
            hits.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        var sub = source.consume(c);
        try {
            source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
            source.dispatch(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER))).toCompletableFuture().join();
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        assertThat(hits.get()).isEqualTo(2);
    }

    @Test
    void doubleConsumeRejected() {
        var source = new SignalSource<>(TraceData.class);
        var sub = source.consume(traces -> ConsumeResult.acceptedStage());
        try {
            assertThatThrownBy(() -> source.consume(traces -> ConsumeResult.acceptedStage()))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @Test
    void dispatchPropagatesConsumerException() {
        var source = new SignalSource<>(TraceData.class);
        TraceConsumer c = traces -> { throw new RuntimeException("boom"); };
        var sub = source.consume(c);
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
