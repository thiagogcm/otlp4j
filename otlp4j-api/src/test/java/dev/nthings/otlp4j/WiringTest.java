package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.PipelineHandle;
import dev.nthings.otlp4j.pipeline.SharedLifecycle;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.ValidationIssue;
import dev.nthings.otlp4j.pipeline.Wiring;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Wiring")
class WiringTest {

    /// Recording stub that appends its label to a shared sequence. A [PipelineHandle] is a
    /// [Lifecycle], so one stub serves both builder phases.
    private static final class RecordingHandle implements PipelineHandle {

        private final String label;
        private final List<String> sequence;

        RecordingHandle(String label, List<String> sequence) {
            this.label = label;
            this.sequence = sequence;
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            sequence.add(label);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> forceFlush(Duration timeout) {
            sequence.add(label + "-flush");
            return CompletableFuture.completedFuture(null);
        }
    }

    @DisplayName("shutdown order: receivers, then pipelines, then owned resources")
    @Test
    void shutdownOrderIsReceiversPipelinesOwned() {
        var sequence = new ArrayList<String>();
        var wiring = Wiring.builder()
                .owns(new RecordingHandle("owned", sequence))
                .pipeline(new RecordingHandle("pipeline", sequence))
                .receiver(new RecordingHandle("receiver", sequence))
                .build();

        wiring.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();

        // Phase order wins over registration order.
        assertThat(sequence).containsExactly("receiver", "pipeline", "owned");
    }

    @DisplayName("shutdown is best-effort and rethrows the first error at the end")
    @Test
    void shutdownIsBestEffortWithFirstError() {
        var sequence = new ArrayList<String>();
        var failing = new Lifecycle() {
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                sequence.add("failing");
                return CompletableFuture.failedFuture(new IllegalStateException("receiver stuck"));
            }
        };
        var wiring = Wiring.builder()
                .receiver(failing)
                .pipeline(new RecordingHandle("pipeline", sequence))
                .owns(new RecordingHandle("owned", sequence))
                .build();

        var stage = wiring.shutdown(Duration.ofSeconds(1)).toCompletableFuture();

        assertThatThrownBy(stage::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("receiver stuck");
        // The failure did not stop the later phases.
        assertThat(sequence).containsExactly("failing", "pipeline", "owned");
    }

    @DisplayName("a second shutdown is a no-op")
    @Test
    void secondShutdownIsNoOp() {
        var sequence = new ArrayList<String>();
        var wiring = Wiring.builder()
                .receiver(new RecordingHandle("receiver", sequence))
                .build();

        wiring.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        wiring.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();

        assertThat(sequence).containsExactly("receiver");
    }

    @DisplayName("a resource registered in two phases is shut down once")
    @Test
    void crossPhaseDedup() {
        var sequence = new ArrayList<String>();
        var receiver = new RecordingHandle("shared", sequence);
        var wiring = Wiring.builder()
                .receiver(receiver)
                .owns(receiver)
                .build();

        wiring.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();

        assertThat(sequence).containsExactly("shared");
    }

    @DisplayName("forceFlush flushes pipelines then owned Lifecycles, not receivers")
    @Test
    void forceFlushOrder() {
        var sequence = new ArrayList<String>();
        var wiring = Wiring.builder()
                .receiver(new RecordingHandle("receiver", sequence))
                .pipeline(new RecordingHandle("pipeline", sequence))
                .owns(new RecordingHandle("owned", sequence))
                .build();

        wiring.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();

        assertThat(sequence).containsExactly("pipeline-flush", "owned-flush");
    }

    @DisplayName("owns(shared) retains once at build and releases on the one shutdown")
    @Test
    void sharedOwnedResourceStaysBalanced() {
        var retains = new AtomicInteger();
        var shutdowns = new AtomicInteger();

        class SharedResource implements SharedLifecycle {
            @Override public void retain() {
                retains.incrementAndGet();
            }
            @Override public void release() {
                retains.decrementAndGet();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                shutdowns.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        }

        var shared = new SharedResource();
        var wiring = Wiring.builder().owns(shared).owns(shared).build();
        assertThat(retains.get()).isEqualTo(1);

        // A transient owner that retains then release()s nets back to the wiring's one stake, without
        // draining: release decrements the count but never closes.
        shared.retain();
        shared.release();
        assertThat(retains.get()).isEqualTo(1);
        assertThat(shutdowns.get()).isZero();

        wiring.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(shutdowns.get()).isEqualTo(1);
    }

    @DisplayName("close() drives a real pipeline end to end")
    @Test
    void closesRealPipeline() {
        var source = new ManualSource<TracesData>();
        var terminalClosed = new AtomicInteger();

        class CloseableTerminal implements TracesSink, AutoCloseable {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public void close() {
                terminalClosed.incrementAndGet();
            }
        }

        var subscription = Pipeline.from(source).to(new CloseableTerminal());
        try (var wiring = Wiring.builder().pipeline(subscription).build()) {
            assertThat(terminalClosed.get()).isZero();
        }
        assertThat(terminalClosed.get()).isEqualTo(1);
    }

    // --- F6: describe() / validate() ---

    /// A no-op ingest-side [Lifecycle], to satisfy the receiver phase in validate() tests.
    private static final class NoopReceiver implements Lifecycle {
        @Override public CompletionStage<Void> shutdown(Duration timeout) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /// An [AutoCloseable] terminal: a visible, non-opaque leaf resource.
    private static final class ClosingTerminal implements TracesSink, AutoCloseable {
        @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
            return ConsumeResult.acceptedStage();
        }
        @Override public void close() {
        }
    }

    /// A plain [Lifecycle] (not [SharedLifecycle]) terminal: attaching one instance to two pipelines
    /// is the DOUBLE_ATTACHED_UNSHARED bug.
    private static final class StatefulSink implements TracesSink, Lifecycle {
        @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
            return ConsumeResult.acceptedStage();
        }
        @Override public CompletionStage<Void> shutdown(Duration timeout) {
            return CompletableFuture.completedFuture(null);
        }
    }

    @DisplayName("describe() dumps receivers, each pipeline's topology, and owned resources")
    @Test
    void describeDumpsTopology() {
        var source = new ManualSource<TracesData>();
        var terminal = new ClosingTerminal();
        var subscription = Pipeline.from(source).to(terminal);
        var wiring = Wiring.builder()
                .receiver(new NoopReceiver())
                .pipeline(subscription)
                .owns(new ClosingTerminal())
                .build();

        var dump = wiring.describe();
        assertThat(dump)
                .contains("wiring:")
                .contains("receivers (1):")
                .contains(NoopReceiver.class.getName())
                .contains("pipelines (1):")
                .contains("pipeline:")
                .contains("terminal: AUTO_CLOSEABLE")
                .contains(ClosingTerminal.class.getName())
                .contains("owned (1):");
    }

    @DisplayName("describe() annotates a shared owned resource with its structural owner count")
    @Test
    void describeAnnotatesSharedOwnerCount() {
        var retains = new AtomicInteger();

        class SharedTerminal implements TracesSink, SharedLifecycle {
            @Override public CompletionStage<ConsumeResult> consume(TracesData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public void retain() {
                retains.incrementAndGet();
            }
            @Override public void release() {
                retains.decrementAndGet();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                return CompletableFuture.completedFuture(null);
            }
        }

        var source = new ManualSource<TracesData>();
        var shared = new SharedTerminal();
        var subscription = Pipeline.from(source).to(shared);
        var wiring = Wiring.builder().pipeline(subscription).owns(shared).build();

        // One pipeline attachment plus the owned registration: two structural owners.
        assertThat(wiring.describe()).contains("[shared, owners=2]");
    }

    @DisplayName("validate() flags an opaque lambda terminal with no owns(), and clears it once owns() is declared")
    @Test
    void validateFlagsOpaqueTerminalWithoutOwns() {
        var opaqueSource = new ManualSource<TracesData>();
        TracesSink lambda = traces -> ConsumeResult.acceptedStage();
        var opaque = Pipeline.from(opaqueSource).to(lambda);

        var declaredSource = new ManualSource<TracesData>();
        var declared = Pipeline.from(declaredSource).owns(new ClosingTerminal()).to(lambda);

        var wiring = Wiring.builder()
                .receiver(new NoopReceiver())
                .pipeline(opaque)
                .pipeline(declared)
                .build();

        var issues = wiring.validate();
        assertThat(issues).extracting(ValidationIssue::kind)
                .containsOnlyOnce(ValidationIssue.Kind.OPAQUE_TERMINAL);
    }

    @DisplayName("validate() flags pipelines declared with no receiver")
    @Test
    void validateFlagsNoReceivers() {
        var source = new ManualSource<TracesData>();
        var subscription = Pipeline.from(source).to(new ClosingTerminal());
        var wiring = Wiring.builder().pipeline(subscription).build();

        assertThat(wiring.validate()).extracting(ValidationIssue::kind)
                .contains(ValidationIssue.Kind.NO_RECEIVERS);
    }

    @DisplayName("validate() flags receivers declared with no pipeline")
    @Test
    void validateFlagsNoPipelines() {
        var wiring = Wiring.builder().receiver(new NoopReceiver()).build();

        assertThat(wiring.validate()).extracting(ValidationIssue::kind)
                .contains(ValidationIssue.Kind.NO_PIPELINES);
    }

    @DisplayName("validate() flags the same non-shared Lifecycle attached to two pipelines")
    @Test
    void validateFlagsDoubleAttachedUnshared() {
        var shared = new StatefulSink();
        var sourceA = new ManualSource<TracesData>();
        var sourceB = new ManualSource<TracesData>();
        var subA = Pipeline.from(sourceA).to(shared);
        var subB = Pipeline.from(sourceB).to(shared);
        var wiring = Wiring.builder()
                .receiver(new NoopReceiver())
                .pipeline(subA)
                .pipeline(subB)
                .build();

        assertThat(wiring.validate()).extracting(ValidationIssue::kind)
                .contains(ValidationIssue.Kind.DOUBLE_ATTACHED_UNSHARED);
    }

    @DisplayName("validate() flags a resource both owned and auto-collected by a pipeline")
    @Test
    void validateFlagsRedundantOwns() {
        var source = new ManualSource<TracesData>();
        var terminal = new ClosingTerminal();
        var subscription = Pipeline.from(source).to(terminal);
        var wiring = Wiring.builder()
                .receiver(new NoopReceiver())
                .pipeline(subscription)
                .owns(terminal)
                .build();

        assertThat(wiring.validate()).extracting(ValidationIssue::kind)
                .contains(ValidationIssue.Kind.REDUNDANT_OWNS);
    }

    @DisplayName("validate() returns no issues for a clean topology")
    @Test
    void validateCleanTopologyIsEmpty() {
        var source = new ManualSource<TracesData>();
        var subscription = Pipeline.from(source).to(new ClosingTerminal());
        var wiring = Wiring.builder()
                .receiver(new NoopReceiver())
                .pipeline(subscription)
                .build();

        assertThat(wiring.validate()).isEmpty();
    }
}
