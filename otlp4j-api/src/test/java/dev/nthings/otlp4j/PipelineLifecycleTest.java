package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.ForceFlushable;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.Source;
import dev.nthings.otlp4j.core.PipelineHandle;
import dev.nthings.otlp4j.core.TraceSink;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Pipeline lifecycle")
class PipelineLifecycleTest {

    @DisplayName("close() shuts the subscription down without error")
    @Test
    void closeCallsShutdown() {
        var source = new ManualSource<TracesData>();
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).to(terminal);
        sub.close();
        // no exception
        assertThat(true).isTrue();
    }

    @DisplayName("forceFlush() delegates to ForceFlushable terminal resources")
    @Test
    void forceFlushDelegatesToFlushableResources() {
        var source = new ManualSource<TracesData>();
        var forceFlushed = new AtomicInteger();
        var closed = new AtomicBoolean();

        class FlushableTerminal implements TraceSink, ForceFlushable, AutoCloseable {
            @Override public CompletionStage<ConsumeResult<TracesData>> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                forceFlushed.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
            @Override public void close() {
                closed.set(true);
            }
        }

        var terminal = new FlushableTerminal();
        var sub = Pipeline.from(source).to(terminal);
        sub.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(forceFlushed.get()).isEqualTo(1);
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(closed.get()).isTrue();
    }

    @DisplayName("shutdown() propagates AutoCloseable close exceptions")
    @Test
    void shutdownPropagatesAutoCloseableExceptions() {
        var source = new ManualSource<TracesData>();
        class FailingTerminal implements TraceSink, AutoCloseable {
            @Override public CompletionStage<ConsumeResult<TracesData>> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public void close() throws Exception {
                throw new IllegalStateException("close failed");
            }
        }
        var sub = Pipeline.from(source).to(new FailingTerminal());
        var stage = sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture();
        assertThatThrownBy(stage::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("close failed");
    }

    @DisplayName("branch() with no peers throws IllegalStateException")
    @Test
    void branchRequiresAtLeastOnePeer() {
        var source = new ManualSource<TracesData>();
        var branch = Pipeline.from(source).branch();
        assertThatThrownBy(branch::join).isInstanceOf(IllegalStateException.class);
    }

    @DisplayName("A throwing transform stage yields a Rejected result")
    @Test
    void pipelineStageThrowingProducesRejected() {
        var source = new ManualSource<TracesData>();
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source)
                .transform(batch -> { throw new RuntimeException("transform exploded"); })
                .to(terminal);
        try {
            var r = source.dispatch(new TracesData(List.of())).toCompletableFuture().join();
            assertThat(r).isInstanceOf(ConsumeResult.Rejected.class);
        } finally {
            sub.close();
        }
    }

    @DisplayName("A null transform result yields a Rejected result")
    @Test
    void nullTransformResultProducesRejected() {
        var source = new ManualSource<TracesData>();
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source)
                .transform(batch -> null)
                .to(terminal);
        try {
            var r = source.dispatch(new TracesData(List.of())).toCompletableFuture().join();
            assertThat(r).isInstanceOf(ConsumeResult.Rejected.class);
            assertThat(((ConsumeResult.Rejected<?>) r).message())
                    .contains("pipeline transform returned null");
        } finally {
            sub.close();
        }
    }

    @DisplayName("owns() drains a registered AutoCloseable not reachable as the terminal")
    @Test
    void ownsDrainsRegisteredResource() {
        var source = new ManualSource<TracesData>();
        var closed = new AtomicBoolean();
        AutoCloseable resource = () -> closed.set(true);
        // The method-reference terminal hides no AutoCloseable, so only owns() can register the drain.
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).owns(resource).to(terminal);

        assertThat(closed.get()).isFalse();
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(closed.get()).isTrue();
    }

    @DisplayName("to(terminal, owner) drains and flushes the owner like owns(owner).to(terminal)")
    @Test
    void twoArgTerminalOwnsResource() {
        var source = new ManualSource<TracesData>();
        var flushed = new AtomicInteger();
        var closed = new AtomicBoolean();

        // A method-reference terminal hides no AutoCloseable; the owner is reached only because the
        // two-arg terminal registers it. The owner is a separate resource, not the terminal itself.
        class OwnedResource implements Drainable, ForceFlushable {
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                flushed.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                closed.set(true);
                return CompletableFuture.completedFuture(null);
            }
        }
        var owner = new OwnedResource();
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).to(terminal, owner);

        sub.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(flushed.get()).isEqualTo(1);

        assertThat(closed.get()).isFalse();
        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(closed.get()).isTrue();
    }

    @DisplayName("shutdown() shares one deadline across owned resources (shrinking remaining)")
    @Test
    void shutdownSharesOneDeadlineAcrossResources() throws Exception {
        var source = new ManualSource<TracesData>();

        // A resource that is also a PipelineHandle records the remaining timeout it was closed with, and
        // burns a small slice of it, so the next resource in line must receive a strictly smaller one.
        class RecordingResource implements PipelineHandle {
            final AtomicReference<Duration> closedWith = new AtomicReference<>();
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                closedWith.set(timeout);
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return CompletableFuture.completedFuture(null);
            }
        }
        var first = new RecordingResource();
        var second = new RecordingResource();
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).owns(first).owns(second).to(terminal);

        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();

        // Both were closed; the second's remaining budget is at most the first's (single shared deadline,
        // not the full timeout handed to each).
        assertThat(first.closedWith.get()).isNotNull();
        assertThat(second.closedWith.get()).isNotNull();
        assertThat(second.closedWith.get()).isLessThan(first.closedWith.get());
        // Sanity: neither received more than the original budget.
        assertThat(first.closedWith.get()).isLessThanOrEqualTo(Duration.ofSeconds(1));
    }

    @DisplayName("shutdown() drains a non-PipelineHandle Drainable with the remaining deadline, not close()")
    @Test
    void shutdownDrainsDrainableWithRemainingDeadline() {
        var source = new ManualSource<TracesData>();

        // A plain AutoCloseable (like OtlpGrpcExporter) that is Drainable must receive the deadline-aware
        // shutdown(Duration), not the fixed-timeout close() that would ignore the pipeline's shared budget.
        class DrainableResource implements Drainable {
            final AtomicReference<Duration> shutdownWith = new AtomicReference<>();
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                shutdownWith.set(timeout);
                return CompletableFuture.completedFuture(null);
            }
            @Override public void close() {
                throw new AssertionError("close() must not be used when Drainable.shutdown is available");
            }
        }
        var resource = new DrainableResource();
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).owns(resource).to(terminal);

        sub.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();

        assertThat(resource.shutdownWith.get())
                .as("the pipeline's remaining budget reached the Drainable instead of its fixed-timeout close()")
                .isNotNull()
                .isLessThanOrEqualTo(Duration.ofSeconds(5));
    }

    @DisplayName("forceFlush() shares one deadline across owned resources (shrinking remaining)")
    @Test
    void forceFlushSharesOneDeadlineAcrossResources() throws Exception {
        var source = new ManualSource<TracesData>();

        class RecordingFlushable implements ForceFlushable, AutoCloseable {
            final AtomicReference<Duration> flushedWith = new AtomicReference<>();
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                flushedWith.set(timeout);
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return CompletableFuture.completedFuture(null);
            }
            @Override public void close() {}
        }
        var first = new RecordingFlushable();
        var second = new RecordingFlushable();
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).owns(first).owns(second).to(terminal);

        sub.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();

        assertThat(first.flushedWith.get()).isNotNull();
        assertThat(second.flushedWith.get()).isNotNull();
        assertThat(second.flushedWith.get()).isLessThan(first.flushedWith.get());
        assertThat(first.flushedWith.get()).isLessThanOrEqualTo(Duration.ofSeconds(1));
    }

    @DisplayName("shutdown() closes later resources even when an earlier one fails")
    @Test
    void shutdownIsBestEffortAcrossResources() {
        var source = new ManualSource<TracesData>();
        var secondClosed = new AtomicBoolean();
        AutoCloseable failing = () -> {
            throw new IllegalStateException("boom");
        };
        AutoCloseable second = () -> secondClosed.set(true);
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).owns(failing).owns(second).to(terminal);

        var stage = sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture();

        // The failing resource is surfaced, but teardown still reached the later resource.
        assertThatThrownBy(stage::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("boom");
        assertThat(secondClosed.get()).isTrue();
    }

    @DisplayName("forceFlush() flushes the source subscription, not only owned resources")
    @Test
    void forceFlushReachesSourceSubscription() {
        var sourceFlushed = new AtomicInteger();
        class FlushingSource implements Source<TracesData> {
            @Override public PipelineHandle subscribe(Sink<? super TracesData> consumer) {
                return new PipelineHandle() {
                    @Override public CompletionStage<Void> shutdown(Duration timeout) {
                        return CompletableFuture.completedFuture(null);
                    }
                    @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                        sourceFlushed.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    }
                };
            }
        }
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(new FlushingSource()).to(terminal);

        sub.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(sourceFlushed.get()).isEqualTo(1);
    }

    @DisplayName("forceFlush() reaches a ForceFlushable registered via owns()")
    @Test
    void forceFlushReachesOwnedFlushable() {
        var source = new ManualSource<TracesData>();
        var flushed = new AtomicInteger();

        class FlushableResource implements AutoCloseable, ForceFlushable {
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                flushed.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
            @Override public void close() {}
        }
        var resource = new FlushableResource();
        TraceSink terminal = traces -> ConsumeResult.acceptedStage();
        var sub = Pipeline.from(source).owns(resource).to(terminal);

        sub.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(flushed.get()).isEqualTo(1);

        sub.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
    }
}
