package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.FanOut;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.TraceSink;
import dev.nthings.otlp4j.processor.Transforms;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Pipeline")
class PipelineTest {

    @DisplayName("Transform and filter stages apply in declared order")
    @Test
    void transformAndFilterApplyInOrder() {
        var source = new ManualSource<TracesData>();
        var captured = new ArrayList<TracesData>();
        TraceSink terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from(source)
                .transform(Transforms.withTracesResourceAttribute("env", AttributeValue.of("prod")))
                .transform(Transforms.keepSpansWhere(span -> span.kind() == Span.Kind.SERVER))
                .filter(traces -> !traces.spans().isEmpty())
                .to(terminal);
        try {
            source.dispatch(Fixtures.traceData(
                    Fixtures.span("a", Span.Kind.SERVER),
                    Fixtures.span("b", Span.Kind.INTERNAL))).toCompletableFuture().join();
            source.dispatch(Fixtures.traceData(
                    Fixtures.span("c", Span.Kind.INTERNAL))).toCompletableFuture().join();
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        assertThat(captured).hasSize(1);
        assertThat(captured.getFirst().spans()).hasSize(1);
        assertThat(captured.getFirst().resourceSpans().getFirst().resource().attributes().get("env"))
                .isInstanceOf(AttributeValue.StringValue.class);
    }

    @DisplayName("FanOut.of fans out each item to every peer consumer")
    @Test
    void fanOutToEveryPeer() {
        var source = new ManualSource<TracesData>();
        var a = new AtomicInteger();
        var b = new AtomicInteger();
        TraceSink peerA = traces -> {
            a.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        TraceSink peerB = traces -> {
            b.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from(source).to(FanOut.of(peerA, peerB));
        try {
            source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        assertThat(a.get()).isEqualTo(1);
        assertThat(b.get()).isEqualTo(1);
    }

    @DisplayName("A transform after a filter is not invoked on a dropped (null) batch")
    @Test
    void transformAfterFilterIsNotInvokedOnDroppedBatch() {
        var source = new ManualSource<TracesData>();
        var observedByTransform = new AtomicInteger();
        var delivered = new ArrayList<TracesData>();
        TraceSink terminal = traces -> {
            delivered.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from(source)
                .filter(traces -> !traces.spans().isEmpty())
                .transform(traces -> {
                    observedByTransform.incrementAndGet();
                    return traces;
                })
                .to(terminal);
        try {
            // An all-dropped batch must short-circuit before the downstream transform, not NPE.
            var result = source.dispatch(Fixtures.traceData()).toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(observedByTransform).hasValue(0);
            assertThat(delivered).isEmpty();
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("A supertype sink can terminate and fan out a subtype pipeline (contravariance)")
    @Test
    void contravariantSinkComposition() {
        var seenByTerminal = new AtomicInteger();
        var seenByPeer = new AtomicInteger();
        // Sink<Object> is a supertype sink; it compiles into to/fanOut/FanOut.of only because they
        // accept Sink<? super TracesData>. No casts.
        Sink<Object> universalTerminal = batch -> {
            seenByTerminal.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        Sink<Object> universalPeer = batch -> {
            seenByPeer.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };

        var batch = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));

        // Both FanOut.of overloads accept Sink<? super TracesData>; consume drives the supertype peer.
        assertThat(FanOut.of(List.of(universalPeer)).consume(batch).toCompletableFuture().join())
                .isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(FanOut.<TracesData>of(universalPeer).consume(batch).toCompletableFuture().join())
                .isInstanceOf(ConsumeResult.Accepted.class);

        // Stage.to accepts the supertype sink directly and via FanOut.of.
        // One consumer slot per ManualSource, so drive each path on its own source.
        var terminalSource = new ManualSource<TracesData>();
        var peerSource = new ManualSource<TracesData>();
        var terminalSub = Pipeline.from(terminalSource).to(universalTerminal);
        var branchSub = Pipeline.from(peerSource).to(FanOut.of(universalPeer));
        try {
            assertThat(terminalSource.dispatch(batch).toCompletableFuture().join())
                    .isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(peerSource.dispatch(batch).toCompletableFuture().join())
                    .isInstanceOf(ConsumeResult.Accepted.class);
        } finally {
            terminalSub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
            branchSub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        // 2 FanOut peers + 1 branch peer; 1 plain terminal.
        assertThat(seenByTerminal).hasValue(1);
        assertThat(seenByPeer).hasValue(3);
    }

    @DisplayName("A synchronous terminal throw becomes a permanent rejection, not an escaped exception")
    @Test
    void synchronousTerminalThrowBecomesPermanentRejection() {
        var source = new ManualSource<TracesData>();
        TraceSink terminal = traces -> {
            throw new RuntimeException("terminal exploded");
        };
        var sub = Pipeline.from(source).to(terminal);
        try {
            var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
            var rejected = (ConsumeResult.Rejected<TracesData>) result;
            // Non-null cause => permanent, not retryable.
            assertThat(rejected.cause()).isInstanceOf(RuntimeException.class).hasMessage("terminal exploded");
            assertThat(rejected.message()).contains("pipeline terminal threw");
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("An asynchronous terminal failure becomes a permanent rejection")
    @Test
    void asynchronousTerminalFailureBecomesPermanentRejection() {
        var source = new ManualSource<TracesData>();
        TraceSink terminal = traces ->
                CompletableFuture.failedFuture(new IllegalStateException("async boom"));
        var sub = Pipeline.from(source).to(terminal);
        try {
            var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
            var rejected = (ConsumeResult.Rejected<TracesData>) result;
            assertThat(rejected.cause()).isInstanceOf(IllegalStateException.class).hasMessage("async boom");
            assertThat(rejected.message()).contains("pipeline terminal failed");
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("An Error completing the terminal stage propagates instead of becoming a rejection")
    @Test
    void asynchronousTerminalErrorPropagates() {
        var source = new ManualSource<TracesData>();
        var overflow = new StackOverflowError("terminal error");
        TraceSink terminal = traces -> CompletableFuture.failedFuture(overflow);
        var sub = Pipeline.from(source).to(terminal);
        try {
            assertThatThrownBy(() -> source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseReference(overflow);
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("A terminal that sneaky-throws a checked exception is normalized and restores the interrupt flag")
    @Test
    void synchronousTerminalCheckedThrowIsNormalizedAndRestoresInterrupt() {
        Thread.interrupted();
        var source = new ManualSource<TracesData>();
        var interrupted = new InterruptedException("terminal interrupted");
        // consume() declares no checked exceptions; sneaky-throw one as a blocking sink might.
        TraceSink terminal = traces -> sneakyThrow(interrupted);
        var sub = Pipeline.from(source).to(terminal);
        try {
            var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    rejected -> assertThat(rejected.cause()).isSameAs(interrupted));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("A terminal that sneaky-throws a bare Throwable is normalized, not escaped")
    @Test
    void synchronousTerminalBareThrowableIsNormalized() {
        var source = new ManualSource<TracesData>();
        var raw = new Throwable("bare throwable");
        TraceSink terminal = traces -> sneakyThrow(raw);
        var sub = Pipeline.from(source).to(terminal);
        try {
            var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    rejected -> assertThat(rejected.cause()).isSameAs(raw));
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("A terminal that returns a null stage is normalized, not escaped as an NPE")
    @Test
    void terminalReturningNullStageIsNormalized() {
        var source = new ManualSource<TracesData>();
        TraceSink terminal = traces -> null;
        var sub = Pipeline.from(source).to(terminal);
        try {
            var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                    rejected -> assertThat(rejected.cause()).isInstanceOf(NullPointerException.class));
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("Shutting down the PipelineHandle detaches the consumer")
    @Test
    void closingSubscriptionDetachesSink() {
        var source = new ManualSource<TracesData>();
        var captured = new ArrayList<TracesData>();
        TraceSink terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from(source).to(terminal);
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(captured).isEmpty();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    private static <T> T sneakyThrow(Throwable failure) {
        return PipelineTest.<RuntimeException, T>sneakyThrow0(failure);
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable, T> T sneakyThrow0(Throwable failure) throws E {
        throw (E) failure;
    }
}
