package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Sink authoring adapters")
class SinkAdaptersTest {

    private static final TracesData BATCH = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));

    private static <T> ConsumeResult<T> consume(Sink<T> sink, T batch) {
        return sink.consume(batch).toCompletableFuture().join();
    }

    @DisplayName("accepting runs the action and accepts on normal return")
    @Test
    void acceptingAcceptsOnNormalReturn() {
        var seen = new AtomicReference<TracesData>();
        Sink<TracesData> sink = Sink.accepting(seen::set);

        var result = consume(sink, BATCH);

        assertThat(seen.get()).isSameAs(BATCH);
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("accepting maps a thrown checked exception to a permanent rejection carrying the cause")
    @Test
    void acceptingMapsThrowToRejectedWithCause() {
        var boom = new IOException("disk full");
        Sink<TracesData> sink = Sink.accepting(batch -> {
            throw boom;
        });

        var result = consume(sink, BATCH);

        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, rejected -> {
            assertThat(rejected.cause()).isSameAs(boom);
            assertThat(rejected.message()).contains("IOException").contains("disk full");
        });
    }

    @DisplayName("accepting lets Errors propagate rather than swallowing them as rejections")
    @Test
    void acceptingLetsErrorsPropagate() {
        var overflow = new StackOverflowError("boom");
        Sink<TracesData> sink = Sink.accepting(batch -> {
            throw overflow;
        });

        try {
            sink.consume(BATCH);
            org.junit.jupiter.api.Assertions.fail("expected the Error to propagate");
        } catch (StackOverflowError thrown) {
            assertThat(thrown).isSameAs(overflow);
        }
    }

    @DisplayName("accepting unwraps CompletionException before building the rejection")
    @Test
    void acceptingUnwrapsCompletionException() {
        var boom = new IllegalStateException("wrapped");
        Sink<TracesData> sink = Sink.accepting(batch -> {
            throw new CompletionException(boom);
        });

        var result = consume(sink, BATCH);

        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, rejected -> {
            assertThat(rejected.cause()).isSameAs(boom);
            assertThat(rejected.message()).contains("IllegalStateException").contains("wrapped");
        });
    }

    @DisplayName("accepting restores the interrupt flag when the action throws InterruptedException")
    @Test
    void acceptingRestoresInterruptFlag() {
        Thread.interrupted();
        var interrupted = new InterruptedException("stop");
        Sink<TracesData> sink = Sink.accepting(batch -> {
            throw interrupted;
        });

        try {
            var result = consume(sink, BATCH);

            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    rejected -> assertThat(rejected.cause()).isSameAs(interrupted));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @DisplayName("accepting restores the interrupt flag when CompletionException wraps InterruptedException")
    @Test
    void acceptingRestoresInterruptFlagFromWrappedFailure() {
        Thread.interrupted();
        var interrupted = new InterruptedException("wrapped stop");
        Sink<TracesData> sink = Sink.accepting(batch -> {
            throw new CompletionException(interrupted);
        });

        try {
            var result = consume(sink, BATCH);

            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    rejected -> assertThat(rejected.cause()).isSameAs(interrupted));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @DisplayName("fromStage accepts when the returned stage completes normally")
    @Test
    void fromStageAcceptsOnNormalCompletion() {
        var seen = new AtomicReference<TracesData>();
        Sink<TracesData> sink = Sink.fromStage(batch -> {
            seen.set(batch);
            return CompletableFuture.completedFuture(null);
        });

        var result = consume(sink, BATCH);

        assertThat(seen.get()).isSameAs(BATCH);
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("fromStage maps an exceptionally completed stage to a rejection with the unwrapped cause")
    @Test
    void fromStageMapsFailureToRejected() {
        var boom = new IllegalStateException("downstream gone");
        Sink<TracesData> sink = Sink.fromStage(batch -> CompletableFuture.failedFuture(boom));

        var result = consume(sink, BATCH);

        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                rejected -> assertThat(rejected.cause()).isSameAs(boom));
    }

    @DisplayName("fromStage unwraps the CompletionException from an asynchronously failed stage")
    @Test
    void fromStageUnwrapsAsyncFailure() {
        var boom = new IllegalStateException("async downstream gone");
        Sink<TracesData> sink = Sink.fromStage(batch -> CompletableFuture.supplyAsync(() -> {
            throw boom;
        }));

        var result = consume(sink, BATCH);

        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, rejected -> {
            // handle() wraps the failure in a CompletionException; the rejection carries the unwrapped cause.
            assertThat(rejected.cause()).isSameAs(boom);
            assertThat(rejected.message()).contains("IllegalStateException").contains("async downstream gone");
        });
    }

    @DisplayName("fromStage maps a synchronous throw to a rejection")
    @Test
    void fromStageMapsSynchronousThrowToRejected() {
        var boom = new IllegalArgumentException("bad batch");
        Sink<TracesData> sink = Sink.fromStage(batch -> {
            throw boom;
        });

        var result = consume(sink, BATCH);

        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                rejected -> assertThat(rejected.cause()).isSameAs(boom));
    }

    @DisplayName("fromStage lets Errors thrown by the action propagate")
    @Test
    void fromStageLetsErrorsPropagate() {
        var overflow = new StackOverflowError("boom");
        Sink<TracesData> sink = Sink.fromStage(batch -> {
            throw overflow;
        });

        try {
            sink.consume(BATCH);
            org.junit.jupiter.api.Assertions.fail("expected the Error to propagate");
        } catch (StackOverflowError thrown) {
            assertThat(thrown).isSameAs(overflow);
        }
    }

    @DisplayName("accepting normalizes a bare Throwable sneaky-thrown by the action")
    @Test
    void acceptingNormalizesBareThrowable() {
        var raw = new Throwable("bare");
        Sink<TracesData> sink = Sink.accepting(batch -> {
            sneakyThrow(raw);
        });

        var result = consume(sink, BATCH);

        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                rejected -> assertThat(rejected.cause()).isSameAs(raw));
    }

    @DisplayName("fromStage lets an Error completing the stage propagate rather than swallowing it")
    @Test
    void fromStageLetsStageErrorsPropagate() {
        var overflow = new StackOverflowError("async boom");
        Sink<TracesData> sink = Sink.fromStage(batch -> CompletableFuture.failedFuture(overflow));

        assertThatThrownBy(() -> sink.consume(BATCH).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseReference(overflow);
    }

    @DisplayName("fromStage unwraps a synchronously thrown CompletionException")
    @Test
    void fromStageUnwrapsSynchronousCompletionException() {
        var boom = new IllegalStateException("sync wrapped");
        Sink<TracesData> sink = Sink.fromStage(batch -> {
            throw new CompletionException(boom);
        });

        var result = consume(sink, BATCH);

        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, rejected -> {
            assertThat(rejected.cause()).isSameAs(boom);
            assertThat(rejected.message()).contains("IllegalStateException").contains("sync wrapped");
        });
    }

    @DisplayName("fromStage restores the interrupt flag when the action throws InterruptedException")
    @Test
    void fromStageRestoresInterruptFlag() {
        Thread.interrupted();
        var interrupted = new InterruptedException("stop");
        Sink<TracesData> sink = Sink.fromStage(batch -> sneakyThrow(interrupted));

        try {
            var result = consume(sink, BATCH);

            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    rejected -> assertThat(rejected.cause()).isSameAs(interrupted));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @DisplayName("fromStage restores the interrupt flag when CompletionException wraps InterruptedException")
    @Test
    void fromStageRestoresInterruptFlagFromWrappedSynchronousFailure() {
        Thread.interrupted();
        var interrupted = new InterruptedException("sync wrapped stop");
        Sink<TracesData> sink = Sink.fromStage(batch -> {
            throw new CompletionException(interrupted);
        });

        try {
            var result = consume(sink, BATCH);

            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    rejected -> assertThat(rejected.cause()).isSameAs(interrupted));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @DisplayName("fromStage restores the interrupt flag when the stage fails with InterruptedException")
    @Test
    void fromStageRestoresInterruptFlagForStageFailure() {
        Thread.interrupted();
        var interrupted = new InterruptedException("async stop");
        Sink<TracesData> sink = Sink.fromStage(batch -> CompletableFuture.failedFuture(interrupted));

        try {
            var result = consume(sink, BATCH);

            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    rejected -> assertThat(rejected.cause()).isSameAs(interrupted));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @DisplayName("fromStage maps a null returned stage to a rejection")
    @Test
    void fromStageMapsNullStageToRejected() {
        Sink<TracesData> sink = Sink.fromStage(batch -> null);

        var result = consume(sink, BATCH);

        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                rejected -> assertThat(rejected.cause()).isInstanceOf(NullPointerException.class));
    }

    @DisplayName("per-signal factories narrow the return type to their SAM")
    @Test
    void perSignalFactoriesNarrowToSam() {
        // Compiles only because the factories return the SAM type the onTraces/... builders require.
        TraceSink trace = TraceSink.accepting(batch -> {});
        MetricSink metric = MetricSink.fromStage(batch -> CompletableFuture.completedFuture(null));
        LogSink logs = LogSink.accepting(batch -> {});
        ProfileSink profiles = ProfileSink.fromStage(batch -> CompletableFuture.completedFuture(null));

        assertThat(trace).isInstanceOf(TraceSink.class);
        assertThat(metric).isInstanceOf(MetricSink.class);
        assertThat(logs).isInstanceOf(LogSink.class);
        assertThat(profiles).isInstanceOf(ProfileSink.class);
        assertThat(consume(trace, BATCH)).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("TraceSink.accepting carries the same throw-to-rejection mapping")
    @Test
    void traceSinkAcceptingMapsThrow() {
        TraceSink sink = TraceSink.accepting(batch -> {
            throw new IOException("nope");
        });

        CompletionStage<ConsumeResult<TracesData>> stage = sink.consume(BATCH);

        assertThat(stage.toCompletableFuture().join()).isInstanceOf(ConsumeResult.Rejected.class);
    }

    private static <T> T sneakyThrow(Throwable failure) {
        return SinkAdaptersTest.<RuntimeException, T>sneakyThrow0(failure);
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable, T> T sneakyThrow0(Throwable failure) throws E {
        throw (E) failure;
    }
}
