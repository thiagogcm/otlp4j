package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.connector.Connectors;
import dev.nthings.otlp4j.connector.FailurePolicy;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Count connectors")
class ConnectorsTest {

    @DisplayName("spanCount connector emits span count as a Sum metric")
    @Test
    void spanCountConnectorEmitsCountMetric() {
        var captured = new ArrayList<MetricsData>();
        MetricSink downstream = metrics -> {
            captured.add(metrics);
            return ConsumeResult.acceptedStage();
        };
        var connector = Connectors.spanCount(downstream);
        var traces = Fixtures.traceData(
                Fixtures.span("a", Span.Kind.SERVER),
                Fixtures.span("b", Span.Kind.SERVER),
                Fixtures.span("c", Span.Kind.INTERNAL));
        var result = connector.consume(traces).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(captured).hasSize(1);
        var metric = captured.getFirst().metrics().getFirst();
        assertThat(metric.name()).isEqualTo("otlp4j.connector.span.count");
        assertThat(longValue(metric)).isEqualTo(3L);
    }

    @DisplayName("logRecordCount connector emits log record count as a Sum metric")
    @Test
    void logRecordCountConnectorEmitsCountMetric() {
        var captured = new ArrayList<MetricsData>();
        MetricSink downstream = metrics -> {
            captured.add(metrics);
            return ConsumeResult.acceptedStage();
        };
        var connector = Connectors.logRecordCount(downstream);
        var logs = Fixtures.logsData(
                Fixtures.logRecord("a", LogRecord.Severity.INFO),
                Fixtures.logRecord("b", LogRecord.Severity.WARN));
        var result = connector.consume(logs).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        var metric = captured.getFirst().metrics().getFirst();
        assertThat(metric.name()).isEqualTo("otlp4j.connector.log.record.count");
        assertThat(longValue(metric)).isEqualTo(2L);
    }

    @DisplayName("BEST_EFFORT (default) accepts input despite downstream Rejected")
    @Test
    void bestEffortAcceptsInputDespiteDownstreamRejected() {
        MetricSink downstream = metrics ->
                CompletableFuture.completedStage(ConsumeResult.rejected("backend down"));
        var connector = Connectors.spanCount(downstream);
        var result = connector.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("FAIL propagates a downstream Rejected onto the input result")
    @Test
    void failPropagatesDownstreamRejected() {
        MetricSink downstream = metrics ->
                CompletableFuture.completedStage(ConsumeResult.rejected("backend down"));
        var connector = Connectors.spanCount(downstream, FailurePolicy.FAIL);
        var result = connector.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("FAIL propagates a downstream Partial onto the input result")
    @Test
    void failPropagatesDownstreamPartial() {
        MetricSink downstream = metrics ->
                CompletableFuture.completedStage(ConsumeResult.partial(1L, "metric pushback"));
        var connector = Connectors.logRecordCount(downstream, FailurePolicy.FAIL);
        var result = connector.consume(Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO)))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("Delta window starts at a real time and the next window is contiguous")
    @Test
    void deltaWindowIsContiguousAcrossFlushes() {
        var captured = new ArrayList<MetricsData>();
        MetricSink downstream = metrics -> {
            captured.add(metrics);
            return ConsumeResult.acceptedStage();
        };
        var connector = Connectors.spanCount(downstream);
        var traces = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        connector.consume(traces).toCompletableFuture().join();
        connector.consume(traces).toCompletableFuture().join();
        assertThat(captured).hasSize(2);
        var first = point(captured.get(0));
        var second = point(captured.get(1));
        assertThat(first.startEpochNanos()).as("first window start is a real wall-clock time, not the epoch").isGreaterThan(0L);
        assertThat(second.startEpochNanos()).as("second window starts where the first ended").isEqualTo(first.epochNanos());
    }

    private static NumberPoint point(MetricsData data) {
        if (data.metrics().getFirst().data() instanceof Metric.Sum sum) {
            return sum.points().getFirst();
        }
        throw new AssertionError("expected a Sum metric");
    }

    private static long longValue(Metric metric) {
        if (metric.data() instanceof Metric.Sum sum
                && !sum.points().isEmpty()
                && sum.points().getFirst().value() instanceof NumberPoint.LongValue v) {
            return v.value();
        }
        return -1L;
    }
}
