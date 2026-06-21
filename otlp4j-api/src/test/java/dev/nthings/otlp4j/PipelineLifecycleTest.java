package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.Source;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.receiver.internal.SignalSource;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Pipeline lifecycle")
class PipelineLifecycleTest {

    @DisplayName("close() shuts the subscription down without error")
    @Test
    void closeCallsShutdown() {
        var source = new SignalSource<>(TraceData.class);
        TraceConsumer terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from((Source<TraceData>) source).to(terminal);
        sub.close();
        // no exception
        assertThat(true).isTrue();
    }

    @DisplayName("forceFlush() delegates to Flushable terminal resources")
    @Test
    void forceFlushDelegatesToFlushableResources() {
        var source = new SignalSource<>(TraceData.class);
        var forceFlushed = new AtomicInteger();
        var closed = new AtomicBoolean();

        class FlushableTerminal implements TraceConsumer, Pipeline.Flushable, AutoCloseable {
            @Override public CompletionStage<ConsumeResult<TraceData>> consume(TraceData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                forceFlushed.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
            @Override public void close() {
                closed.set(true);
            }
        }

        var terminal = new FlushableTerminal();
        var sub = Pipeline.from((Source<TraceData>) source).to(terminal);
        sub.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(forceFlushed.get()).isEqualTo(1);
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(closed.get()).isTrue();
    }

    @DisplayName("shutdown() propagates AutoCloseable close exceptions")
    @Test
    void shutdownPropagatesAutoCloseableExceptions() {
        var source = new SignalSource<>(TraceData.class);
        class FailingTerminal implements TraceConsumer, AutoCloseable {
            @Override public CompletionStage<ConsumeResult<TraceData>> consume(TraceData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public void close() throws Exception {
                throw new IllegalStateException("close failed");
            }
        }
        var sub = Pipeline.from((Source<TraceData>) source).to(new FailingTerminal());
        var stage = sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture();
        assertThatThrownBy(stage::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("close failed");
    }

    @DisplayName("branch() with no peers throws IllegalStateException")
    @Test
    void branchRequiresAtLeastOnePeer() {
        var source = new SignalSource<>(TraceData.class);
        var branch = Pipeline.from((Source<TraceData>) source).branch();
        assertThatThrownBy(branch::join).isInstanceOf(IllegalStateException.class);
    }

    @DisplayName("A throwing transform stage yields a Rejected result")
    @Test
    void pipelineStageThrowingProducesRejected() {
        var source = new SignalSource<>(TraceData.class);
        TraceConsumer terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from((Source<TraceData>) source)
                .transform(batch -> { throw new RuntimeException("transform exploded"); })
                .to(terminal);
        try {
            var r = source.dispatch(new TraceData(List.of())).toCompletableFuture().join();
            assertThat(r).isInstanceOf(ConsumeResult.Rejected.class);
        } finally {
            sub.close();
        }
    }
}
