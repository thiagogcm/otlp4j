package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.ops.Batcher;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("Batcher as a Processor")
class BatcherProcessorTest {

    @DisplayName("A mid-chain Batcher coalesces many inputs into a single downstream delivery")
    @Test
    void midChainBatcherCoalesces() {
        var source = new ManualSource<TracesData>();
        var captured = new ArrayList<TracesData>();
        TracesSink terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };

        var batcher = Batcher.forTraces()
                .flushThreshold(3)
                .queueCapacity(16)
                .buildProcessor();
        var sub = Pipeline.from(source).via(batcher).to(terminal);
        try {
            for (var i = 0; i < 3; i++) {
                source.dispatch(Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER)))
                        .toCompletableFuture().join();
            }
            await().atMost(Duration.ofSeconds(2)).until(() -> !captured.isEmpty());
        } finally {
            sub.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        }
        assertThat(captured).hasSize(1);
        assertThat(captured.getFirst().spans()).hasSize(3);
    }

    @DisplayName("An unbound Batcher used as a bare Sink without a bound downstream throws")
    @Test
    void unboundBatcherAsBareSinkThrows() {
        try (var batcher = Batcher.forTraces().buildProcessor()) {
            assertThatThrownBy(() ->
                    batcher.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("without a bound downstream");
        }
    }

    @DisplayName("Binding a Batcher to two distinct downstreams fails fast")
    @Test
    void bindingTwoDistinctDownstreamsThrows() {
        TracesSink first = traces -> ConsumeResult.acceptedStage();
        TracesSink second = traces -> ConsumeResult.acceptedStage();
        try (var batcher = Batcher.forTraces().buildProcessor()) {
            batcher.process(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)), first)
                    .toCompletableFuture().join();
            assertThatThrownBy(() -> batcher.process(
                    Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER)), second))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("two downstreams");
        }
    }

    @DisplayName("A concurrent first process from two chains binds exactly one downstream; the other fails fast")
    @Test
    @Timeout(15)
    void concurrentFirstProcessBindsExactlyOnce() throws Exception {
        TracesSink first = traces -> ConsumeResult.acceptedStage();
        TracesSink second = traces -> ConsumeResult.acceptedStage();
        try (var batcher = Batcher.forTraces().queueCapacity(16).buildProcessor()) {
            var barrier = new CyclicBarrier(2);
            var successes = new AtomicInteger();
            var failure = new AtomicReference<Throwable>();

            Runnable bindFirst = raceBind(batcher, first, barrier, successes, failure);
            Runnable bindSecond = raceBind(batcher, second, barrier, successes, failure);

            var t1 = new Thread(bindFirst);
            var t2 = new Thread(bindSecond);
            t1.start();
            t2.start();
            t1.join();
            t2.join();

            // Exactly one thread bound and enqueued; the other observed the race and threw.
            assertThat(successes.get()).isEqualTo(1);
            assertThat(failure.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("two downstreams");
        }
    }

    private static Runnable raceBind(
            Batcher<TracesData> batcher,
            TracesSink sink,
            CyclicBarrier barrier,
            AtomicInteger successes,
            AtomicReference<Throwable> failure) {
        return () -> {
            try {
                barrier.await();
                batcher.process(Fixtures.traceData(Fixtures.span("x", Span.Kind.SERVER)), sink)
                        .toCompletableFuture().join();
                successes.incrementAndGet();
            } catch (IllegalStateException e) {
                failure.set(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                failure.set(e);
            }
        };
    }

    @DisplayName("Batcher.asSink binds the downstream and stays non-cascading (does not shut the downstream)")
    @Test
    void asSinkIsNonCascading() {
        var downstreamShutdowns = new AtomicInteger();

        class LifecycleSink implements TracesSink, Lifecycle {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                downstreamShutdowns.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        }

        var downstream = new LifecycleSink();
        var batcher = Batcher.forTraces().build();
        Sink<TracesData> bound = batcher.asSink(downstream);
        // The override returns the Batcher itself, not a cascading BoundProcessorSink.
        assertThat(bound).isSameAs(batcher);

        batcher.shutdown(Duration.ofSeconds(2)).toCompletableFuture().join();
        // Batcher drains its own queue but never shuts its downstream (documented contract).
        assertThat(downstreamShutdowns.get()).isZero();
    }
}
