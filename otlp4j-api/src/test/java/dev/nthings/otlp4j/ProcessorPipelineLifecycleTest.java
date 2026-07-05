package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.FanOut;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.Processor;
import dev.nthings.otlp4j.pipeline.SharedLifecycle;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.TracesProcessor;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.Wiring;
import dev.nthings.otlp4j.pipeline.ops.AbstractProcessor;
import dev.nthings.otlp4j.pipeline.ops.Transforms;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Processor pipeline lifecycle")
class ProcessorPipelineLifecycleTest {

    /// A processor that records its dispatch and its teardown into shared sequences, forwarding each
    /// batch unchanged. Being a [Lifecycle], it is auto-collected mid-chain.
    private static final class RecordingStage implements Processor<TracesData>, Lifecycle {
        private final String name;
        private final List<String> dispatch;
        private final List<String> teardown;

        RecordingStage(String name, List<String> dispatch, List<String> teardown) {
            this.name = name;
            this.dispatch = dispatch;
            this.teardown = teardown;
        }

        @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
            dispatch.add(name);
            return next.consume(batch);
        }
        @Override public CompletionStage<Void> shutdown(Duration timeout) {
            teardown.add(name + "-shutdown");
            return CompletableFuture.completedFuture(null);
        }
        @Override public CompletionStage<Void> forceFlush(Duration timeout) {
            teardown.add(name + "-flush");
            return CompletableFuture.completedFuture(null);
        }
    }

    @DisplayName("Multi-via: dispatch order is p1->p2->terminal with each next reaching the full downstream segment")
    @Test
    void multiViaDispatchOrderAndSegmentThreading() {
        var source = new ManualSource<TracesData>();
        var dispatch = new ArrayList<String>();
        var teardown = new ArrayList<String>();
        var captured = new ArrayList<TracesData>();

        var p1 = new RecordingStage("p1", dispatch, teardown);
        var p2 = new RecordingStage("p2", dispatch, teardown);

        class RecordingTerminal implements TracesSink, Lifecycle {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                dispatch.add("terminal");
                captured.add(batch);
                return ConsumeResult.acceptedStage();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                teardown.add("terminal-shutdown");
                return CompletableFuture.completedFuture(null);
            }
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                teardown.add("terminal-flush");
                return CompletableFuture.completedFuture(null);
            }
        }

        // Transforms bracketing each via prove the sealed segments compose end to end: the terminal
        // must observe BOTH resource attributes, so neither processor's next was a truncated segment.
        var sub = Pipeline.from(source)
                .transform(Transforms.withTracesResourceAttribute("first", AttributeValue.of("1")))
                .via(p1)
                .transform(Transforms.withTracesResourceAttribute("second", AttributeValue.of("2")))
                .via(p2)
                .to(new RecordingTerminal());

        source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();

        assertThat(dispatch).containsExactly("p1", "p2", "terminal");
        var attributes = captured.getFirst().resourceSpans().getFirst().resource().attributes();
        assertThat(attributes.get("first")).isInstanceOf(AttributeValue.StringValue.class);
        assertThat(attributes.get("second")).isInstanceOf(AttributeValue.StringValue.class);

        sub.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(teardown).containsExactly("p1-flush", "p2-flush", "terminal-flush");

        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(teardown).containsExactly(
                "p1-flush", "p2-flush", "terminal-flush",
                "p1-shutdown", "p2-shutdown", "terminal-shutdown");
    }

    @DisplayName("forceFlush reaches a mid-chain processor before the terminal it feeds")
    @Test
    void forceFlushReachesProcessorThenTerminal() {
        var source = new ManualSource<TracesData>();
        var teardown = new ArrayList<String>();
        var dispatch = new ArrayList<String>();

        class RecordingTerminal implements TracesSink, Lifecycle {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                teardown.add("terminal-flush");
                return CompletableFuture.completedFuture(null);
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                return CompletableFuture.completedFuture(null);
            }
        }

        var sub = Pipeline.from(source)
                .via(new RecordingStage("p", dispatch, teardown))
                .to(new RecordingTerminal());
        sub.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(teardown).containsExactly("p-flush", "terminal-flush");

        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
    }

    @DisplayName("An AutoCloseable-only processor is auto-collected via via() with no owns() declaration")
    @Test
    void autoCloseableProcessorIsAutoCollected() {
        var source = new ManualSource<TracesData>();
        var closed = new AtomicBoolean();

        class CloseableProcessor implements Processor<TracesData>, AutoCloseable {
            @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
                return next.consume(batch);
            }
            @Override public void close() {
                closed.set(true);
            }
        }

        TracesSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).via(new CloseableProcessor()).to(terminal);
        assertThat(closed.get()).isFalse();
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(closed.get()).isTrue();
    }

    @DisplayName("via(p).to(FanOut.of(a, b)) drains the processor before both fan-out peers")
    @Test
    void viaThenFanOutDrainOrder() {
        var source = new ManualSource<TracesData>();
        var teardown = new ArrayList<String>();
        var dispatch = new ArrayList<String>();

        class RecordingPeer implements TracesSink, Lifecycle {
            private final String name;
            RecordingPeer(String name) {
                this.name = name;
            }
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                teardown.add(name);
                return CompletableFuture.completedFuture(null);
            }
        }

        var sub = Pipeline.from(source)
                .via(new RecordingStage("p", dispatch, teardown))
                .to(FanOut.of(new RecordingPeer("a"), new RecordingPeer("b")));

        source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();

        assertThat(teardown).containsExactly("p-shutdown", "a", "b");
    }

    @DisplayName("A processor reached via via() and Wiring.owns() drains exactly once (idempotent shutdown)")
    @Test
    void viaAndOwnsDrainsOnce() {
        var source = new ManualSource<TracesData>();
        var shutdowns = new AtomicInteger();

        class CountingProcessor extends AbstractProcessor<TracesData> {
            CountingProcessor() {
                super("counting");
            }
            @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
                bindNext(next);
                return next.consume(batch);
            }
            @Override protected CompletionStage<Void> onShutdown(Duration timeout) {
                shutdowns.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        }

        TracesSink terminal = traces -> ConsumeResult.acceptedStage();
        var processor = new CountingProcessor();
        var sub = Pipeline.from(source).via(processor).to(terminal);

        // The processor is drained by the subscription (phase 2) AND directly owned (phase 3); Wiring
        // cannot see inside the subscription, so idempotent shutdown is the guard against a double drain.
        var wiring = Wiring.builder().pipeline(sub).owns(processor).build();
        wiring.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();

        assertThat(shutdowns.get()).isEqualTo(1);
    }

    @DisplayName("A SharedLifecycle processor attached to two subscriptions is retained twice and torn down once")
    @Test
    void sharedProcessorRetainedTwiceTornDownOnce() {
        var retains = new AtomicInteger();
        var teardowns = new AtomicInteger();

        class SharedProcessor implements Processor<TracesData>, SharedLifecycle {
            private final AtomicInteger refs = new AtomicInteger();
            @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
                return next.consume(batch);
            }
            @Override public void retain() {
                retains.incrementAndGet();
                refs.incrementAndGet();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                if (refs.decrementAndGet() == 0) {
                    teardowns.incrementAndGet();
                }
                return CompletableFuture.completedFuture(null);
            }
        }

        var shared = new SharedProcessor();
        var sourceA = new ManualSource<TracesData>();
        var sourceB = new ManualSource<TracesData>();
        TracesSink terminalA = traces -> ConsumeResult.acceptedStage();
        TracesSink terminalB = traces -> ConsumeResult.acceptedStage();

        // Each subscription retains the shared processor once at build.
        var subA = Pipeline.from(sourceA).via(shared).to(terminalA);
        var subB = Pipeline.from(sourceB).via(shared).to(terminalB);
        assertThat(retains.get()).isEqualTo(2);

        // Each subscription issues one shutdown; the reference count releases the resource once.
        subA.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(teardowns.get()).isZero();
        subB.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(teardowns.get()).isEqualTo(1);
    }

    @DisplayName("A single lambda via() stage leaves the fast path terminal semantics intact")
    @Test
    void singleLambdaViaForwards() {
        var source = new ManualSource<TracesData>();
        var captured = new ArrayList<TracesData>();
        TracesSink terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        TracesProcessor forwarding = (batch, next) -> next.consume(batch);
        var sub = Pipeline.from(source).via(forwarding).to(terminal);
        try {
            source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        assertThat(captured).hasSize(1);
    }
}
