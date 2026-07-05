package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.ops.AbstractProcessor;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("AbstractProcessor")
class AbstractProcessorTest {

    /// A buffering processor that captures its continuation on the first process and holds each batch
    /// without forwarding, flushing the buffer into [#next] on shutdown and forceFlush.
    private static final class BufferingProcessor extends AbstractProcessor<TracesData> {

        private final List<TracesData> buffer = new ArrayList<>();
        private final AtomicInteger shutdowns = new AtomicInteger();
        private final AtomicInteger flushes = new AtomicInteger();

        BufferingProcessor() {
            super("buffering");
        }

        /// Exposes the protected captured continuation for assertions.
        Sink<? super TracesData> exposedNext() {
            return next();
        }

        @Override
        public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
            bindNext(next);
            buffer.add(batch);
            return ConsumeResult.acceptedStage();
        }

        @Override
        protected CompletionStage<Void> onShutdown(Duration timeout) {
            shutdowns.incrementAndGet();
            return flushBuffer();
        }

        @Override
        protected CompletionStage<Void> onForceFlush(Duration timeout) {
            flushes.incrementAndGet();
            return flushBuffer();
        }

        private CompletionStage<Void> flushBuffer() {
            var next = next();
            if (next == null) {
                return CompletableFuture.completedFuture(null);
            }
            for (var batch : buffer) {
                next.consume(batch);
            }
            buffer.clear();
            return CompletableFuture.completedFuture(null);
        }
    }

    @DisplayName("onShutdown flushes buffered batches into the captured next()")
    @Test
    void onShutdownFlushesToNext() {
        var captured = new ArrayList<TracesData>();
        TracesSink downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var processor = new BufferingProcessor();
        // process binds next() and buffers without forwarding.
        processor.process(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)), downstream)
                .toCompletableFuture().join();
        processor.process(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER)), downstream)
                .toCompletableFuture().join();
        assertThat(captured).isEmpty();

        processor.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(processor.shutdowns.get()).isEqualTo(1);
        assertThat(captured).hasSize(2);
    }

    @DisplayName("onForceFlush flushes buffered batches into next() without tearing the processor down")
    @Test
    void onForceFlushFlushesToNext() {
        var captured = new ArrayList<TracesData>();
        TracesSink downstream = traces -> {
            captured.add(traces);
            return ConsumeResult.acceptedStage();
        };
        var processor = new BufferingProcessor();
        processor.process(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)), downstream)
                .toCompletableFuture().join();

        processor.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(processor.flushes.get()).isEqualTo(1);
        assertThat(captured).hasSize(1);

        // Still usable after a flush: it was not shut down.
        processor.process(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER)), downstream)
                .toCompletableFuture().join();
        processor.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(captured).hasSize(2);
    }

    @DisplayName("next() is null before the first process and returns the captured continuation after")
    @Test
    void nextIsCapturedOnFirstProcess() {
        var seen = new AtomicInteger();
        TracesSink downstream = traces -> {
            seen.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };

        class ForwardingProcessor extends AbstractProcessor<TracesData> {
            ForwardingProcessor() {
                super("forwarding");
            }

            Sink<? super TracesData> exposedNext() {
                return next();
            }

            @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
                bindNext(next);
                return next.consume(batch);
            }
        }

        var processor = new ForwardingProcessor();
        assertThat(processor.exposedNext()).isNull();
        processor.process(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)), downstream)
                .toCompletableFuture().join();
        assertThat(processor.exposedNext()).isSameAs(downstream);
    }

    @DisplayName("A second shutdown is idempotent: onShutdown runs exactly once")
    @Test
    void shutdownIsIdempotent() {
        var processor = new BufferingProcessor();
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        processor.process(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)), downstream)
                .toCompletableFuture().join();

        processor.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        processor.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();

        assertThat(processor.shutdowns.get()).isEqualTo(1);
    }

    @DisplayName("Binding the processor to two distinct continuations fails fast (single-attachment)")
    @Test
    void bindNextToTwoDistinctThrows() {
        var processor = new BufferingProcessor();
        TracesSink first = traces -> ConsumeResult.acceptedStage();
        TracesSink second = traces -> ConsumeResult.acceptedStage();

        processor.process(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)), first)
                .toCompletableFuture().join();
        // A second, different continuation is an illegal second attachment.
        assertThatThrownBy(() -> processor.process(
                Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER)), second))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("two downstreams");
    }

    @DisplayName("Re-binding the same continuation is accepted (idempotent bind)")
    @Test
    void bindNextToSameContinuationIsAccepted() {
        var processor = new BufferingProcessor();
        TracesSink downstream = traces -> ConsumeResult.acceptedStage();
        processor.process(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)), downstream)
                .toCompletableFuture().join();
        // Same continuation again: no throw.
        processor.process(Fixtures.traceData(Fixtures.span("b", Span.Kind.SERVER)), downstream)
                .toCompletableFuture().join();
        assertThat(processor.exposedNext()).isSameAs(downstream);
    }

    @DisplayName("Mid-chain, the base does not cascade shutdown to its downstream")
    @Test
    void midChainShutdownDoesNotCascadeToDownstream() {
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

        class PlainProcessor extends AbstractProcessor<TracesData> {
            PlainProcessor() {
                super("plain");
            }
            @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
                bindNext(next);
                return next.consume(batch);
            }
        }

        var downstream = new LifecycleSink();
        var processor = new PlainProcessor();
        processor.process(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)), downstream)
                .toCompletableFuture().join();

        processor.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        // The base drains only its own local state; the downstream is the pipeline's to drain.
        assertThat(downstreamShutdowns.get()).isZero();
    }

    @DisplayName("A stateful processor attached via via() is auto-collected and drains before the terminal")
    @Test
    void statefulProcessorDrainsBeforeTerminalViaPipeline() {
        var source = new ManualSource<TracesData>();
        var order = new ArrayList<String>();
        var captured = new ArrayList<TracesData>();

        class RecordingTerminal implements TracesSink, Lifecycle {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                captured.add(batch);
                return ConsumeResult.acceptedStage();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                order.add("terminal");
                return CompletableFuture.completedFuture(null);
            }
        }

        class RecordingProcessor extends AbstractProcessor<TracesData> {
            RecordingProcessor() {
                super("recording");
            }
            @Override public CompletionStage<ConsumeResult> process(TracesData batch, Sink<? super TracesData> next) {
                bindNext(next);
                return next.consume(batch);
            }
            @Override protected CompletionStage<Void> onShutdown(Duration timeout) {
                order.add("processor");
                return CompletableFuture.completedFuture(null);
            }
        }

        var sub = Pipeline.from(source).via(new RecordingProcessor()).to(new RecordingTerminal());
        source.dispatch(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();

        assertThat(captured).hasSize(1);
        assertThat(order).containsExactly("processor", "terminal");
    }
}
