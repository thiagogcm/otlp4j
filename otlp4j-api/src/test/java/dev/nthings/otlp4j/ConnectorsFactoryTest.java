package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.ops.Connector;
import dev.nthings.otlp4j.pipeline.ops.Connectors;
import dev.nthings.otlp4j.pipeline.ops.FailurePolicy;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Exercises the general [Connectors#of] factory: a conversion is a factory call, not a subclass.
@DisplayName("Connectors.of factory")
class ConnectorsFactoryTest {

    private static MetricsData oneMetric(TracesData batch) {
        return Fixtures.metricsData(Fixtures.metric("spans=" + batch.spanCount()));
    }

    private static TracesData traces() {
        return Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
    }

    @DisplayName("of() delivers the derived batch to the downstream and accepts the input")
    @Test
    void deliversDerivedBatch() {
        var captured = new ArrayList<MetricsData>();
        Sink<MetricsData> downstream = m -> {
            captured.add(m);
            return ConsumeResult.acceptedStage();
        };
        var connector = Connectors.of(
                "t2m", Signal.TRACES, Signal.METRICS, ConnectorsFactoryTest::oneMetric, downstream);
        var result = connector.consume(traces()).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(captured).hasSize(1);
    }

    @DisplayName("a null derive result emits nothing and accepts the input")
    @Test
    void nullDeriveDrops() {
        var delivered = new AtomicInteger();
        Sink<MetricsData> downstream = m -> {
            delivered.incrementAndGet();
            return ConsumeResult.acceptedStage();
        };
        var connector = Connectors.of(
                "t2m", Signal.TRACES, Signal.METRICS, (TracesData t) -> null, downstream);
        var result = connector.consume(traces()).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(delivered.get()).isZero();
    }

    @DisplayName("the policy matrix: default BEST_EFFORT accepts, FAIL propagates a downstream rejection")
    @Test
    void policyMatrix() {
        Sink<MetricsData> rejecting = m -> CompletableFuture.completedStage(ConsumeResult.retryable("backend down"));

        var bestEffort = Connectors.of(
                "t2m", Signal.TRACES, Signal.METRICS, ConnectorsFactoryTest::oneMetric, rejecting);
        assertThat(bestEffort.consume(traces()).toCompletableFuture().join())
                .isInstanceOf(ConsumeResult.Accepted.class);

        var fail = Connectors.of(
                "t2m", Signal.TRACES, Signal.METRICS, ConnectorsFactoryTest::oneMetric, rejecting, FailurePolicy.FAIL);
        assertThat(fail.consume(traces()).toCompletableFuture().join())
                .isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("toString names the (input -> output) signal pair")
    @Test
    void toStringShowsPair() {
        var connector = Connectors.of(
                "t2m", Signal.TRACES, Signal.METRICS, ConnectorsFactoryTest::oneMetric,
                (Sink<MetricsData>) m -> ConsumeResult.acceptedStage());
        assertThat(connector.toString()).contains("t2m").contains("traces->metrics");
    }

    @DisplayName("the list overload fans the derived batch to every downstream")
    @Test
    void listOverloadFansToEveryDownstream() {
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
        Connector<TracesData, MetricsData> connector = Connectors.of(
                "t2m", Signal.TRACES, Signal.METRICS, ConnectorsFactoryTest::oneMetric,
                List.of(peerA, peerB), FailurePolicy.BEST_EFFORT);
        connector.consume(traces()).toCompletableFuture().join();
        assertThat(a).hasSize(1);
        assertThat(b).hasSize(1);
    }
}
