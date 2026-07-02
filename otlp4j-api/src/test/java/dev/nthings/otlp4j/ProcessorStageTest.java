package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.Processor;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Exercises [Pipeline.Stage#process]: async stage composition, drop short-circuiting, the
/// failure-normalization matrix, and lifecycle auto-collection of closeable processors.
@DisplayName("Pipeline async Processor stage")
class ProcessorStageTest {

    private static TracesData batch() {
        return Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
    }

    @DisplayName("process() runs between surrounding sync stages, in order")
    @Test
    void processComposesInOrderWithSyncStages() {
        var order = new ArrayList<String>();
        var source = new ManualSource<TracesData>();
        var seen = new AtomicReference<TracesData>();
        var sub = Pipeline.from(source)
                .transform(t -> {
                    order.add("before");
                    return t;
                })
                .process(t -> {
                    order.add("process");
                    return CompletableFuture.completedFuture(t);
                })
                .transform(t -> {
                    order.add("after");
                    return t;
                })
                .to((TracesSink) t -> {
                    seen.set(t);
                    return ConsumeResult.acceptedStage();
                });
        try {
            var sent = batch();
            var result = source.dispatch(sent).toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(order).containsExactly("before", "process", "after");
            assertThat(seen.get()).isSameAs(sent);
        } finally {
            sub.close();
        }
    }

    @DisplayName("the acknowledgement awaits a processor completing on another thread")
    @Test
    void ackAwaitsAsyncCompletion() {
        var source = new ManualSource<TracesData>();
        var seen = new AtomicReference<TracesData>();
        var sub = Pipeline.from(source)
                .process(t -> CompletableFuture.supplyAsync(() -> t,
                        CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS)))
                .to((TracesSink) t -> {
                    seen.set(t);
                    return ConsumeResult.acceptedStage();
                });
        try {
            var sent = batch();
            var result = source.dispatch(sent).toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(seen.get()).isSameAs(sent);
        } finally {
            sub.close();
        }
    }

    @DisplayName("two processors chain; the second sees the first's output")
    @Test
    void processorsChain() {
        var source = new ManualSource<TracesData>();
        var replacement = batch();
        var secondInput = new AtomicReference<TracesData>();
        var sub = Pipeline.from(source)
                .process(t -> CompletableFuture.completedFuture(replacement))
                .process(t -> {
                    secondInput.set(t);
                    return CompletableFuture.completedFuture(t);
                })
                .to((TracesSink) t -> ConsumeResult.acceptedStage());
        try {
            var result = source.dispatch(batch()).toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(secondInput.get()).isSameAs(replacement);
        } finally {
            sub.close();
        }
    }

    @DisplayName("a batch dropped by an earlier filter never reaches the processor")
    @Test
    void droppedBatchSkipsProcessor() {
        var source = new ManualSource<TracesData>();
        var processed = new AtomicInteger();
        var delivered = new AtomicInteger();
        var sub = Pipeline.from(source)
                .filter(t -> false)
                .process(t -> {
                    processed.incrementAndGet();
                    return CompletableFuture.completedFuture(t);
                })
                .to((TracesSink) t -> {
                    delivered.incrementAndGet();
                    return ConsumeResult.acceptedStage();
                });
        try {
            var result = source.dispatch(batch()).toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(processed.get()).isZero();
            assertThat(delivered.get()).isZero();
        } finally {
            sub.close();
        }
    }

    @DisplayName("a filter after the processor drops the batch before the terminal")
    @Test
    void filterAfterProcessorDrops() {
        var source = new ManualSource<TracesData>();
        var delivered = new AtomicInteger();
        var sub = Pipeline.from(source)
                .process(t -> CompletableFuture.completedFuture(t))
                .filter(t -> false)
                .to((TracesSink) t -> {
                    delivered.incrementAndGet();
                    return ConsumeResult.acceptedStage();
                });
        try {
            var result = source.dispatch(batch()).toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(delivered.get()).isZero();
        } finally {
            sub.close();
        }
    }

    @DisplayName("a throwing processor rejects the batch permanently")
    @Test
    void throwingProcessorRejects() {
        assertRejectedPermanently(
                t -> { throw new IllegalStateException("processor boom"); },
                "processor boom");
    }

    @DisplayName("a failed processor stage rejects the batch permanently")
    @Test
    void failedProcessorStageRejects() {
        assertRejectedPermanently(
                t -> CompletableFuture.failedFuture(new IllegalStateException("stage failed")),
                "stage failed");
    }

    @DisplayName("a null processor stage rejects the batch permanently")
    @Test
    void nullProcessorStageRejects() {
        assertRejectedPermanently(t -> null, "processor returned a null stage");
    }

    @DisplayName("a processor completing null rejects the batch permanently")
    @Test
    void nullProcessorValueRejects() {
        assertRejectedPermanently(
                t -> CompletableFuture.completedFuture(null),
                "processors are 1-to-1");
    }

    @DisplayName("a transform violating the non-null contract after a processor still rejects")
    @Test
    void nullTransformAfterProcessorRejects() {
        var source = new ManualSource<TracesData>();
        var sub = Pipeline.from(source)
                .process(t -> CompletableFuture.completedFuture(t))
                .transform(t -> null)
                .to((TracesSink) t -> ConsumeResult.acceptedStage());
        try {
            var result = source.dispatch(batch()).toCompletableFuture().join();
            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, r -> {
                assertThat(r.retryable()).isFalse();
                assertThat(r.message()).contains("transforms are 1-to-1");
            });
        } finally {
            sub.close();
        }
    }

    @DisplayName("an Error thrown by a processor propagates instead of becoming a rejection")
    @Test
    void errorPropagates() {
        var source = new ManualSource<TracesData>();
        var sub = Pipeline.from(source)
                .process(t -> { throw new AssertionError("must escape"); })
                .to((TracesSink) t -> ConsumeResult.acceptedStage());
        try {
            var stage = source.dispatch(batch()).toCompletableFuture();
            assertThatThrownBy(stage::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(AssertionError.class);
        } finally {
            sub.close();
        }
    }

    @DisplayName("an InterruptedException failure restores the interrupt flag")
    @Test
    void interruptedExceptionRestoresFlag() {
        var source = new ManualSource<TracesData>();
        var sub = Pipeline.from(source)
                .process(t -> CompletableFuture.failedFuture(new InterruptedException("interrupted")))
                .to((TracesSink) t -> ConsumeResult.acceptedStage());
        try {
            var result = source.dispatch(batch()).toCompletableFuture().join();
            assertThat(Thread.interrupted()).isTrue();
            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    r -> assertThat(r.retryable()).isFalse());
        } finally {
            sub.close();
        }
    }

    @DisplayName("a terminal failure after a processor still maps to a permanent rejection")
    @Test
    void terminalFailureAfterProcessorRejects() {
        var source = new ManualSource<TracesData>();
        var sub = Pipeline.from(source)
                .process(t -> CompletableFuture.completedFuture(t))
                .to((TracesSink) t -> CompletableFuture.failedFuture(new IllegalStateException("terminal down")));
        try {
            var result = source.dispatch(batch()).toCompletableFuture().join();
            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, r -> {
                assertThat(r.retryable()).isFalse();
                assertThat(r.message()).contains("terminal down");
            });
        } finally {
            sub.close();
        }
    }

    @DisplayName("an AutoCloseable processor is auto-drained on shutdown, before the terminal")
    @Test
    void closeableProcessorAutoDrainedBeforeTerminal() {
        var source = new ManualSource<TracesData>();
        var order = new ArrayList<String>();

        class CloseableProcessor implements Processor<TracesData>, AutoCloseable {
            @Override public CompletionStage<TracesData> process(TracesData batch) {
                return CompletableFuture.completedFuture(batch);
            }
            @Override public void close() {
                order.add("processor");
            }
        }
        class CloseableTerminal implements TracesSink, AutoCloseable {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public void close() {
                order.add("terminal");
            }
        }

        var sub = Pipeline.from(source)
                .process(new CloseableProcessor())
                .to(new CloseableTerminal());
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(order).containsExactly("processor", "terminal");
    }

    @DisplayName("a processor followed by a FanOut terminal delivers to every peer")
    @Test
    void processorWithFanOutTerminal() {
        var source = new ManualSource<TracesData>();
        var hits = new AtomicInteger();
        TracesSink peer = t -> {
            hits.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from(source)
                .process(t -> CompletableFuture.completedFuture(t))
                .to(dev.nthings.otlp4j.pipeline.FanOut.of(List.of(peer, peer)));
        try {
            var result = source.dispatch(batch()).toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(hits.get()).isEqualTo(2);
        } finally {
            sub.close();
        }
    }

    private static void assertRejectedPermanently(Processor<TracesData> processor, String messagePart) {
        var source = new ManualSource<TracesData>();
        var delivered = new AtomicBoolean();
        var sub = Pipeline.from(source)
                .process(processor)
                .to((TracesSink) t -> {
                    delivered.set(true);
                    return ConsumeResult.acceptedStage();
                });
        try {
            var result = source.dispatch(batch()).toCompletableFuture().join();
            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, r -> {
                assertThat(r.retryable()).isFalse();
                assertThat(r.message()).contains(messagePart);
            });
            assertThat(delivered).isFalse();
        } finally {
            sub.close();
        }
    }
}
