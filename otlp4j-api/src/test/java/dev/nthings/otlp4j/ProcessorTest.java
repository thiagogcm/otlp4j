package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.Processor;
import dev.nthings.otlp4j.pipeline.SharedLifecycle;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.TracesProcessor;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.ops.Processors;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Processor")
class ProcessorTest {

    @DisplayName("A stateless lambda processor attached via via() forwards each batch to next")
    @Test
    void lambdaProcessorForwardsViaVia() {
        var source = new ManualSource<TracesData>();
        var captured = new ArrayList<TracesData>();
        TracesSink terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        // A bare lambda TracesProcessor: forwards unconditionally.
        TracesProcessor forwarding = (batch, next) -> next.consume(batch);
        var sub = Pipeline.from(source).via(forwarding).to(terminal);
        try {
            source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        assertThat(captured).hasSize(1);
        assertThat(captured.getFirst().spans()).hasSize(1);
    }

    @DisplayName("A processor that returns accepted without calling next drops the batch")
    @Test
    void processorDropsBatch() {
        var source = new ManualSource<TracesData>();
        var captured = new ArrayList<TracesData>();
        TracesSink terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        // Drops every batch: accepts upstream but never forwards.
        TracesProcessor dropping = (batch, next) -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).via(dropping).to(terminal);
        try {
            var r = source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            assertThat(r).isInstanceOf(ConsumeResult.Accepted.class);
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        assertThat(captured).isEmpty();
    }

    @DisplayName("Processors.pruning forwards the pruned batch and drops on a null mapping")
    @Test
    void pruningForwardsAndDrops() {
        var source = new ManualSource<TracesData>();
        var captured = new ArrayList<TracesData>();
        TracesSink terminal = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        // Drops any batch whose first span is INTERNAL (null mapping); forwards otherwise.
        Processor<TracesData> pruner = Processors.pruning("drop-internal",
                traces -> traces.spans().getFirst().kind() == Span.Kind.INTERNAL ? null : traces);
        var sub = Pipeline.from(source).via(pruner).to(terminal);
        try {
            source.dispatch(Fixtures.traceData(Fixtures.span("keep", Span.Kind.SERVER)))
                    .toCompletableFuture().join();
            var dropped = source.dispatch(Fixtures.traceData(Fixtures.span("drop", Span.Kind.INTERNAL)))
                    .toCompletableFuture().join();
            assertThat(dropped).isInstanceOf(ConsumeResult.Accepted.class);
        } finally {
            sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        }
        assertThat(captured).hasSize(1);
        assertThat(captured.getFirst().spans().getFirst().name()).isEqualTo("keep");
    }

    @DisplayName("Processors.of and pruning carry the given name via toString for diagnostics")
    @Test
    void namedProcessorsSurfaceName() {
        Processor<TracesData> named = Processors.of("my-processor", (batch, next) -> next.consume(batch));
        Processor<TracesData> pruner = Processors.pruning("my-pruner", traces -> traces);
        assertThat(named.toString()).isEqualTo("my-processor");
        assertThat(pruner.toString()).isEqualTo("my-pruner");
    }

    @DisplayName("Processing.run passes a processor's own ack (Partial) through verbatim")
    @Test
    void normalizationPassesAckThrough() {
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        var partial = ConsumeResult.partial(2L, "two rejected");
        // A processor that returns its own non-Accepted ack; the boundary must not overwrite it.
        Processor<TracesData> proc = (batch, next) -> CompletableFuture.completedFuture(partial);
        var result = proc.asSink(downstream)
                .consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(result).isSameAs(partial);
    }

    @DisplayName("Processing.run normalizes a synchronous throw into a permanent rejection")
    @Test
    void normalizationSynchronousThrow() {
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        Processor<TracesData> proc = (batch, next) -> {
            throw new RuntimeException("processor exploded");
        };
        var result = proc.asSink(downstream)
                .consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, rejected -> {
            assertThat(rejected.cause()).isInstanceOf(RuntimeException.class).hasMessage("processor exploded");
            assertThat(rejected.message()).contains("pipeline processor threw");
        });
    }

    @DisplayName("Processing.run normalizes a null stage into a permanent rejection carrying an NPE")
    @Test
    void normalizationNullStage() {
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        Processor<TracesData> proc = (batch, next) -> null;
        var result = proc.asSink(downstream)
                .consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                rejected -> assertThat(rejected.cause()).isInstanceOf(NullPointerException.class));
    }

    @DisplayName("Processing.run normalizes an asynchronously-failed stage into a permanent rejection")
    @Test
    void normalizationAsyncFailure() {
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        Processor<TracesData> proc = (batch, next) ->
                CompletableFuture.failedFuture(new IllegalStateException("async boom"));
        var result = proc.asSink(downstream)
                .consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, rejected -> {
            assertThat(rejected.cause()).isInstanceOf(IllegalStateException.class).hasMessage("async boom");
            assertThat(rejected.message()).contains("pipeline processor failed");
        });
    }

    @DisplayName("Processing.run lets an Error propagate instead of turning it into a rejection")
    @Test
    void normalizationErrorPropagates() {
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        var overflow = new StackOverflowError("processor error");
        Processor<TracesData> proc = (batch, next) -> CompletableFuture.failedFuture(overflow);
        assertThatThrownBy(() -> proc.asSink(downstream)
                .consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseReference(overflow);
    }

    @DisplayName("The default asSink cascade drains the processor before the downstream")
    @Test
    void asSinkCascadeDrainsProcessorThenDownstream() {
        var order = new ArrayList<String>();

        class LifecycleProcessor implements Processor<TracesData>, Lifecycle {
            @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
                return next.consume(batch);
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                order.add("processor-shutdown");
                return CompletableFuture.completedFuture(null);
            }
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                order.add("processor-flush");
                return CompletableFuture.completedFuture(null);
            }
        }

        class LifecycleSink implements TracesSink, Lifecycle {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                order.add("downstream-shutdown");
                return CompletableFuture.completedFuture(null);
            }
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                order.add("downstream-flush");
                return CompletableFuture.completedFuture(null);
            }
        }

        var sink = new LifecycleProcessor().asSink(new LifecycleSink());
        ((Lifecycle) sink).forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        ((Lifecycle) sink).shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();

        assertThat(order).containsExactly(
                "processor-flush", "downstream-flush", "processor-shutdown", "downstream-shutdown");
    }

    @DisplayName("The default asSink retains a SharedLifecycle on the processor side and the downstream side")
    @Test
    void asSinkRetainsSharedOnBothSides() {
        var processorRetains = new AtomicInteger();
        var downstreamRetains = new AtomicInteger();

        class SharedProcessor implements Processor<TracesData>, SharedLifecycle {
            @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
                return next.consume(batch);
            }
            @Override public void retain() {
                processorRetains.incrementAndGet();
            }
            @Override public void release() {
                processorRetains.decrementAndGet();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                return CompletableFuture.completedFuture(null);
            }
        }

        class SharedSink implements TracesSink, SharedLifecycle {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public void retain() {
                downstreamRetains.incrementAndGet();
            }
            @Override public void release() {
                downstreamRetains.decrementAndGet();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                return CompletableFuture.completedFuture(null);
            }
        }

        var sink = new SharedProcessor().asSink(new SharedSink());
        ((SharedLifecycle) sink).retain();
        assertThat(processorRetains.get()).isEqualTo(1);
        assertThat(downstreamRetains.get()).isEqualTo(1);
    }

    @DisplayName("The default asSink release()s a SharedLifecycle on both sides without draining either")
    @Test
    void asSinkReleasesSharedOnBothSides() {
        var processorRefs = new AtomicInteger();
        var downstreamRefs = new AtomicInteger();
        var shutdowns = new AtomicInteger();

        class SharedProcessor implements Processor<TracesData>, SharedLifecycle {
            @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
                return next.consume(batch);
            }
            @Override public void retain() {
                processorRefs.incrementAndGet();
            }
            @Override public void release() {
                processorRefs.decrementAndGet();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                shutdowns.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        }

        class SharedSink implements TracesSink, SharedLifecycle {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public void retain() {
                downstreamRefs.incrementAndGet();
            }
            @Override public void release() {
                downstreamRefs.decrementAndGet();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                shutdowns.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        }

        var sink = new SharedProcessor().asSink(new SharedSink());
        ((SharedLifecycle) sink).retain();
        ((SharedLifecycle) sink).release();
        assertThat(processorRefs.get()).isZero();
        assertThat(downstreamRefs.get()).isZero();
        assertThat(shutdowns.get()).isZero();
    }

    @DisplayName("A stateless (non-Lifecycle) asSink cascade shuts and flushes cleanly with nothing to drain")
    @Test
    void asSinkStatelessCascadeIsNoOp() {
        var seen = new AtomicInteger();
        TracesSink downstream = traces -> {
            seen.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        // Neither the lambda processor nor the lambda downstream is a Lifecycle/SharedLifecycle:
        // every instanceof arm in the bound sink is false.
        Processor<TracesData> proc = (batch, next) -> next.consume(batch);
        var sink = proc.asSink(downstream);

        var batch = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        assertThat(sink.consume(batch).toCompletableFuture().join()).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(seen.get()).isEqualTo(1);

        ((SharedLifecycle) sink).retain();   // no shared side: no-op, no throw
        ((Lifecycle) sink).forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        ((Lifecycle) sink).shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        // Nothing to assert beyond clean completion; the downstream was never a Lifecycle to drain.
        assertThat(List.of(sink)).hasSize(1);
    }
}
