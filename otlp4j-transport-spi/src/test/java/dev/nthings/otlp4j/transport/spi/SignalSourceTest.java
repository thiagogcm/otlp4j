package dev.nthings.otlp4j.transport.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("SignalSource")
class SignalSourceTest {

    @DisplayName("dispatch returns a retryable rejection when no consumer is attached")
    @Test
    void dispatchWithoutSinkReturnsRetryableRejected() {
        var source = new SignalSource<>(TracesData.class);
        var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, rejected -> {
            assertThat(rejected.message())
                    .contains("no consumer attached for TracesData")
                    .contains("discard()");
            assertThat(rejected.cause()).isNull();
        });
    }

    @DisplayName("discard explicitly accepts and drops batches")
    @Test
    void discardAcceptsAndDropsBatches() {
        var source = new SignalSource<>(TracesData.class);
        var discard = source.discard();
        try {
            var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        } finally {
            discard.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("dispatch invokes the attached consumer once per batch")
    @Test
    void dispatchInvokesAttachedSink() {
        var source = new SignalSource<>(TracesData.class);
        var hits = new AtomicInteger();
        TraceSink c = traces -> {
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
        var source = new SignalSource<>(TracesData.class);
        var sub = source.subscribe(traces -> ConsumeResult.acceptedStage());
        try {
            assertThatThrownBy(() -> source.subscribe(traces -> ConsumeResult.acceptedStage()))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("dispatch propagates a synchronous subscriber throw verbatim (transport-level failure)")
    @Test
    void dispatchPropagatesSinkException() {
        var source = new SignalSource<>(TracesData.class);
        TraceSink c = traces -> { throw new RuntimeException("boom"); };
        var sub = source.subscribe(c);
        try {
            assertThatThrownBy(() ->
                    source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("boom");
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("dispatch propagates an asynchronous subscriber failure verbatim (transport-level failure)")
    @Test
    void dispatchPropagatesAsyncSinkFailure() {
        var source = new SignalSource<>(TracesData.class);
        TraceSink c = traces -> CompletableFuture.failedFuture(new IllegalStateException("async boom"));
        var sub = source.subscribe(c);
        try {
            var stage = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)));
            assertThatThrownBy(() -> stage.toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("async boom");
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }
}
