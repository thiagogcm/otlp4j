package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.Source;
import dev.nthings.otlp4j.pipeline.Subscription;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.processor.Transforms;
import dev.nthings.otlp4j.receiver.internal.SignalSource;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PipelineTest {

    @Test
    void transformAndFilterApplyInOrder() {
        var source = new SignalSource<>(TraceData.class);
        var captured = new ArrayList<TraceData>();
        TraceConsumer terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from((Source<TraceData>) source)
                .transform(Transforms.setTraceResourceAttribute("env", AttributeValue.of("prod")))
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
        assertThat(captured.get(0).spans()).hasSize(1);
        assertThat(captured.get(0).resourceSpans().get(0).resource().attributes().get("env"))
                .isInstanceOf(AttributeValue.StringValue.class);
    }

    @Test
    void branchFansOutToEveryPeer() {
        var source = new SignalSource<>(TraceData.class);
        var a = new AtomicInteger();
        var b = new AtomicInteger();
        TraceConsumer peerA = traces -> {
            a.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        TraceConsumer peerB = traces -> {
            b.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        Subscription sub = Pipeline.from((Source<TraceData>) source).branch()
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

    @Test
    void tapErrorsDoNotAffectMainPath() {
        var source = new SignalSource<>(TraceData.class);
        var captured = new ArrayList<TraceData>();
        TraceConsumer terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from((Source<TraceData>) source)
                .tap(traces -> {
                    throw new RuntimeException("tap exploded");
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

    @Test
    void closingSubscriptionDetachesConsumer() {
        var source = new SignalSource<>(TraceData.class);
        var captured = new ArrayList<TraceData>();
        TraceConsumer terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var sub = Pipeline.from((Source<TraceData>) source).to(terminal);
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        var result = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(captured).isEmpty();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }
}
