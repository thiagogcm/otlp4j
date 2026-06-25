package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.processor.Transforms;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Pipeline")
class PipelineTest {

    @DisplayName("Transform and filter stages apply in declared order")
    @Test
    void transformAndFilterApplyInOrder() {
        var source = new ManualSource<TraceData>();
        var captured = new ArrayList<TraceData>();
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

    @DisplayName("branch() fans out each item to every peer consumer")
    @Test
    void branchFansOutToEveryPeer() {
        var source = new ManualSource<TraceData>();
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
        var sub = Pipeline.from(source).branch()
                .fanOut(peerA)
                .fanOut(peerB)
                .join();
        try {
            source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER))).toCompletableFuture().join();
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        assertThat(a.get()).isEqualTo(1);
        assertThat(b.get()).isEqualTo(1);
    }

    @DisplayName("Exceptions thrown by a peek observer do not break the main path")
    @Test
    void peekErrorsDoNotAffectMainPath() {
        var source = new ManualSource<TraceData>();
        var captured = new ArrayList<TraceData>();
        TraceSink terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from(source)
                .peek(traces -> {
                    throw new RuntimeException("peek exploded");
                })
                .to(terminal);
        try {
            var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
            assertThat(captured).hasSize(1);
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
    }

    @DisplayName("A transform after a filter is not invoked on a dropped (null) batch")
    @Test
    void transformAfterFilterIsNotInvokedOnDroppedBatch() {
        var source = new ManualSource<TraceData>();
        var observedByTransform = new AtomicInteger();
        var delivered = new ArrayList<TraceData>();
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

    @DisplayName("Shutting down the Subscription detaches the consumer")
    @Test
    void closingSubscriptionDetachesSink() {
        var source = new ManualSource<TraceData>();
        var captured = new ArrayList<TraceData>();
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
}
