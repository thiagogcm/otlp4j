package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.SharedLifecycle;
import dev.nthings.otlp4j.pipeline.ops.Connector;
import dev.nthings.otlp4j.pipeline.ops.FailurePolicy;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Exercises the multi-downstream [Connector] form: the derived batch fans to every downstream via
/// [dev.nthings.otlp4j.pipeline.FanOut], results merge with FanOut semantics, the lifecycle cascade
/// reaches every peer, and the intentional two-or-more-peer Error-isolation divergence holds.
@DisplayName("Connector multi-downstream")
class ConnectorMultiDownstreamTest {

    /// A traces-to-metrics connector over a downstream list, always deriving one metrics batch.
    private static final class FanConnector extends Connector<TracesData, MetricsData> {
        FanConnector(List<? extends Sink<? super MetricsData>> downstreams, FailurePolicy policy) {
            super("fan", downstreams, policy);
        }

        @Override
        protected @Nullable MetricsData derive(TracesData batch) {
            return Fixtures.metricsData(Fixtures.metric("count"));
        }
    }

    private static TracesData traces() {
        return Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
    }

    @DisplayName("the derived batch reaches every downstream and the input is accepted")
    @Test
    void bothDownstreamsReceive() {
        var a = new ArrayList<MetricsData>();
        var b = new ArrayList<MetricsData>();
        Sink<MetricsData> peerA = m -> {
            a.add(m);
            return ConsumeResult.acceptedStage();
        };
        Sink<MetricsData> peerB = m -> {
            b.add(m);
            return ConsumeResult.acceptedStage();
        };
        var connector = new FanConnector(List.of(peerA, peerB), FailurePolicy.FAIL);
        var result = connector.consume(traces()).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(a).hasSize(1);
        assertThat(b).hasSize(1);
    }

    @DisplayName("the merged result follows fanOutMerge (worst-case, the max rejected count)")
    @Test
    void mergedResultFollowsFanOutMerge() {
        Sink<MetricsData> partialThree = m -> CompletableFuture.completedStage(ConsumeResult.partial(3L, "three"));
        Sink<MetricsData> partialSeven = m -> CompletableFuture.completedStage(ConsumeResult.partial(7L, "seven"));
        var connector = new FanConnector(List.of(partialThree, partialSeven), FailurePolicy.FAIL);
        var result = connector.consume(traces()).toCompletableFuture().join();
        // FanOut merges to the max rejected count (7, not 3 and not the 10 a sum would give); under
        // FAIL the connector forwards that partial as a retryable rejection naming 7 items.
        assertThat(result).isInstanceOfSatisfying(ConsumeResult.Rejected.class, r -> {
            assertThat(r.retryable()).isTrue();
            assertThat(r.message()).contains("7 item(s)");
        });
    }

    @DisplayName("retain, release, and shutdown cascade to every downstream peer")
    @Test
    void lifecycleCascadesToEveryPeer() {
        var retainsA = new AtomicInteger();
        var shutdownsA = new AtomicInteger();
        var retainsB = new AtomicInteger();
        var shutdownsB = new AtomicInteger();
        var connector = new FanConnector(
                List.of(new SharedPeer(retainsA, shutdownsA), new SharedPeer(retainsB, shutdownsB)),
                FailurePolicy.BEST_EFFORT);

        connector.retain();
        assertThat(retainsA.get()).isEqualTo(1);
        assertThat(retainsB.get()).isEqualTo(1);

        connector.release();
        assertThat(retainsA.get()).isZero();
        assertThat(retainsB.get()).isZero();

        connector.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(shutdownsA.get()).isEqualTo(1);
        assertThat(shutdownsB.get()).isEqualTo(1);
    }

    @DisplayName("an empty downstream list is rejected at construction")
    @Test
    void emptyListRejected() {
        assertThatThrownBy(() -> new FanConnector(List.of(), FailurePolicy.BEST_EFFORT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one downstream");
    }

    @DisplayName("with two or more downstreams a peer Error is captured, not rethrown (fan isolation)")
    @Test
    void peerErrorIsCapturedNotRethrown() {
        Sink<MetricsData> healthy = m -> ConsumeResult.acceptedStage();
        Sink<MetricsData> erroring = m -> {
            throw new AssertionError("one peer's bug must not abort the healthy peer");
        };
        var connector = new FanConnector(List.of(erroring, healthy), FailurePolicy.FAIL);
        // Unlike the single-downstream form (ConnectorSpiTest.downstreamErrorPropagates), the fan
        // isolates the Error: consume() does NOT throw; the merged failure is governed by policy.
        assertThatCode(() -> connector.consume(traces()).toCompletableFuture().join())
                .doesNotThrowAnyException();
        assertThat(connector.consume(traces()).toCompletableFuture().join())
                .isInstanceOf(ConsumeResult.Rejected.class);
    }

    /// A downstream that is both a [Sink] and a [SharedLifecycle], counting retains and shutdowns.
    private static final class SharedPeer implements Sink<MetricsData>, SharedLifecycle {
        private final AtomicInteger retains;
        private final AtomicInteger shutdowns;

        SharedPeer(AtomicInteger retains, AtomicInteger shutdowns) {
            this.retains = retains;
            this.shutdowns = shutdowns;
        }

        @Override public CompletionStage<ConsumeResult> consume(MetricsData batch) {
            return ConsumeResult.acceptedStage();
        }

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
}
