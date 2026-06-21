package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.connector.LogRecordCountConnector;
import dev.nthings.otlp4j.connector.SpanCountConnector;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorsTest {

    @Test
    void spanCountConnectorEmitsCountMetric() {
        var captured = new ArrayList<MetricsData>();
        MetricConsumer downstream = metrics -> {
            captured.add(metrics);
            return ConsumeResult.acceptedStage();
        };
        var connector = new SpanCountConnector(downstream);
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

    @Test
    void logRecordCountConnectorEmitsCountMetric() {
        var captured = new ArrayList<MetricsData>();
        MetricConsumer downstream = metrics -> {
            captured.add(metrics);
            return ConsumeResult.acceptedStage();
        };
        var connector = new LogRecordCountConnector(downstream);
        var logs = Fixtures.logsData(
                Fixtures.logRecord("a", LogRecord.Severity.INFO),
                Fixtures.logRecord("b", LogRecord.Severity.WARN));
        var result = connector.consume(logs).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        var metric = captured.getFirst().metrics().getFirst();
        assertThat(metric.name()).isEqualTo("otlp4j.connector.log.record.count");
        assertThat(longValue(metric)).isEqualTo(2L);
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
