package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.AsyncTransform;
import dev.nthings.otlp4j.pipeline.FanOut;
import dev.nthings.otlp4j.pipeline.Pipeline;
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

/// Exercises [Pipeline.Stage#transformAsync]: async stage composition, drop short-circuiting, the
/// failure-normalization matrix, and lifecycle auto-collection of closeable async transforms.
@DisplayName("Pipeline async transform stage")
class AsyncTransformStageTest {

    private static TracesData batch() {
        return Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
    }

    @DisplayName("transformAsync() runs between surrounding sync stages, in order")
    @Test
    void transformAsyncComposesInOrderWithSyncStages() {
        var order = new ArrayList<String>();
        var source = new ManualSource<TracesData>();
        var seen = new AtomicReference<TracesData>();
        var sub = Pipeline.from(source)
                .transform(t -> {
                    order.add("before");
                    return t;
                })
                .transformAsync(t -> {
                    order.add("transformAsync");
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
            assertThat(order).containsExactly("before", "transformAsync", "after");
            assertThat(seen.get()).isSameAs(sent);
        } finally {
            sub.close();
        }
    }

    @DisplayName("the acknowledgement awaits an async transform completing on another thread")
    @Test
    void ackAwaitsAsyncCompletion() {
        var source = new ManualSource<TracesData>();
        var seen = new AtomicReference<TracesData>();
        var sub = Pipeline.from(source)
                .transformAsync(t -> CompletableFuture.supplyAsync(() -> t,
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

    @DisplayName("two async transforms chain; the second sees the first's output")
    @Test
    void asyncTransformsChain() {
        var source = new ManualSource<TracesData>();
        var replacement = batch();
        var secondInput = new AtomicReference<TracesData>();
        var sub = Pipeline.from(source)
                .transformAsync(t -> CompletableFuture.completedFuture(replacement))
                .transformAsync(t -> {
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

    @DisplayName("a batch dropped by an earlier filter never reaches the async transform")
    @Test
    void droppedBatchSkipsAsyncTransform() {
        var source = new ManualSource<TracesData>();
        var processed = new AtomicInteger();
        var delivered = new AtomicInteger();
        var sub = Pipeline.from(source)
                .filter(t -> false)
                .transformAsync(t -> {
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

    @DisplayName("a filter after the async transform drops the batch before the terminal")
    @Test
    void filterAfterAsyncTransformDrops() {
        var source = new ManualSource<TracesData>();
        var delivered = new AtomicInteger();
        var sub = Pipeline.from(source)
                .transformAsync(t -> CompletableFuture.completedFuture(t))
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

    @DisplayName("a throwing async transform rejects the batch permanently")
    @Test
    void throwingAsyncTransformRejects() {
        assertRejectedPermanently(
                t -> { throw new IllegalStateException("async transform boom"); },
                "async transform boom");
    }

    @DisplayName("a failed async transform stage rejects the batch permanently")
    @Test
    void failedAsyncTransformStageRejects() {
        assertRejectedPermanently(
                t -> CompletableFuture.failedFuture(new IllegalStateException("stage failed")),
                "stage failed");
    }

    @DisplayName("a null async transform stage rejects the batch permanently")
    @Test
    void nullAsyncTransformStageRejects() {
        assertRejectedPermanently(t -> null, "async transform returned a null stage");
    }

    @DisplayName("an async transform completing null rejects the batch permanently")
    @Test
    void nullAsyncTransformValueRejects() {
        assertRejectedPermanently(
                t -> CompletableFuture.completedFuture(null),
                "async transforms are 1-to-1");
    }

    @DisplayName("a transform violating the non-null contract after an async transform still rejects")
    @Test
    void nullTransformAfterAsyncTransformRejects() {
        var source = new ManualSource<TracesData>();
        var sub = Pipeline.from(source)
                .transformAsync(t -> CompletableFuture.completedFuture(t))
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

    @DisplayName("an Error thrown by an async transform propagates instead of becoming a rejection")
    @Test
    void errorPropagates() {
        var source = new ManualSource<TracesData>();
        var sub = Pipeline.from(source)
                .transformAsync(t -> { throw new AssertionError("must escape"); })
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
                .transformAsync(t -> CompletableFuture.failedFuture(new InterruptedException("interrupted")))
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

    @DisplayName("a terminal failure after an async transform still maps to a permanent rejection")
    @Test
    void terminalFailureAfterAsyncTransformRejects() {
        var source = new ManualSource<TracesData>();
        var sub = Pipeline.from(source)
                .transformAsync(t -> CompletableFuture.completedFuture(t))
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

    @DisplayName("an AutoCloseable async transform is auto-drained on shutdown, before the terminal")
    @Test
    void closeableAsyncTransformAutoDrainedBeforeTerminal() {
        var source = new ManualSource<TracesData>();
        var order = new ArrayList<String>();

        class CloseableAsyncTransform implements AsyncTransform<TracesData>, AutoCloseable {
            @Override public CompletionStage<TracesData> applyAsync(TracesData batch) {
                return CompletableFuture.completedFuture(batch);
            }
            @Override public void close() {
                order.add("asyncTransform");
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
                .transformAsync(new CloseableAsyncTransform())
                .to(new CloseableTerminal());
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(order).containsExactly("asyncTransform", "terminal");
    }

    @DisplayName("an async transform followed by a FanOut terminal delivers to every peer")
    @Test
    void asyncTransformWithFanOutTerminal() {
        var source = new ManualSource<TracesData>();
        var hits = new AtomicInteger();
        TracesSink peer = t -> {
            hits.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from(source)
                .transformAsync(t -> CompletableFuture.completedFuture(t))
                .to(FanOut.of(List.of(peer, peer)));
        try {
            var result = source.dispatch(batch()).toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(hits.get()).isEqualTo(2);
        } finally {
            sub.close();
        }
    }

    private static void assertRejectedPermanently(AsyncTransform<TracesData> asyncTransform, String messagePart) {
        var source = new ManualSource<TracesData>();
        var delivered = new AtomicBoolean();
        var sub = Pipeline.from(source)
                .transformAsync(asyncTransform)
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
