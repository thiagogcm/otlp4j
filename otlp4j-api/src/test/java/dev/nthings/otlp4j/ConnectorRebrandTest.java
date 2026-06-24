package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.connector.Connectors;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Cross-signal rebranding: a downstream metric failure must not be relabeled as rejected spans/logs.
@DisplayName("Connector result rebranding")
class ConnectorRebrandTest {

    @DisplayName("spanCount connector accepts input despite downstream Partial")
    @Test
    void spanCountConnectorAcceptsInputDespiteDownstreamPartial() {
        MetricSink downstream = metrics -> CompletableFuture.completedStage(
                ConsumeResult.partial(2L, "metric backend pushback"));
        var connector = Connectors.spanCount(downstream);
        var r = connector.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(r)
                .as("input trace batch is always accepted; downstream metric partial does not relabel as rejected spans")
                .isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("spanCount connector accepts input despite downstream Rejected")
    @Test
    void spanCountConnectorAcceptsInputDespiteDownstreamRejected() {
        MetricSink downstream = metrics -> CompletableFuture.completedStage(
                ConsumeResult.rejected("backend down"));
        var connector = Connectors.spanCount(downstream);
        var r = connector.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(r).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("logRecordCount connector accepts input despite downstream Partial")
    @Test
    void logRecordCountConnectorAcceptsInputDespiteDownstreamPartial() {
        MetricSink downstream = metrics -> CompletableFuture.completedStage(
                ConsumeResult.partial(1L, "metric pushback"));
        var connector = Connectors.logRecordCount(downstream);
        var r = connector.consume(Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO)))
                .toCompletableFuture().join();
        assertThat(r).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("logRecordCount connector accepts input despite downstream Rejected")
    @Test
    void logRecordCountConnectorAcceptsInputDespiteDownstreamRejected() {
        MetricSink downstream = metrics -> CompletableFuture.completedStage(
                ConsumeResult.rejected("backend down"));
        var connector = Connectors.logRecordCount(downstream);
        var r = connector.consume(Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO)))
                .toCompletableFuture().join();
        assertThat(r).isInstanceOf(ConsumeResult.Accepted.class);
    }
}
