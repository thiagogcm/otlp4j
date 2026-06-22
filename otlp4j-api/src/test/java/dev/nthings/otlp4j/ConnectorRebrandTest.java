package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.connector.Connectors;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Cross-signal rebranding of partial/rejected counts at the connector boundary would
/// misattribute rejected metric data points as rejected spans/log records.
@DisplayName("Connector result rebranding")
class ConnectorRebrandTest {

    @DisplayName("spanCount connector accepts input despite downstream Partial")
    @Test
    void spanCountConnectorAcceptsInputDespiteDownstreamPartial() {
        MetricConsumer downstream = metrics -> CompletableFuture.completedStage(
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
        MetricConsumer downstream = metrics -> CompletableFuture.completedStage(
                ConsumeResult.rejected("backend down"));
        var connector = Connectors.spanCount(downstream);
        var r = connector.consume(Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)))
                .toCompletableFuture().join();
        assertThat(r).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("logRecordCount connector accepts input despite downstream Partial")
    @Test
    void logRecordCountConnectorAcceptsInputDespiteDownstreamPartial() {
        MetricConsumer downstream = metrics -> CompletableFuture.completedStage(
                ConsumeResult.partial(1L, "metric pushback"));
        var connector = Connectors.logRecordCount(downstream);
        var r = connector.consume(Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO)))
                .toCompletableFuture().join();
        assertThat(r).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("logRecordCount connector accepts input despite downstream Rejected")
    @Test
    void logRecordCountConnectorAcceptsInputDespiteDownstreamRejected() {
        MetricConsumer downstream = metrics -> CompletableFuture.completedStage(
                ConsumeResult.rejected("backend down"));
        var connector = Connectors.logRecordCount(downstream);
        var r = connector.consume(Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO)))
                .toCompletableFuture().join();
        assertThat(r).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("Connectors expose downstream for graph introspection")
    @Test
    void connectorsExposeDownstreamForGraphIntrospection() {
        MetricConsumer downstream = metrics -> ConsumeResult.acceptedStage();
        var span = Connectors.spanCount(downstream);
        var log = Connectors.logRecordCount(downstream);
        assertThat(span.downstream()).isSameAs(downstream);
        assertThat(log.downstream()).isSameAs(downstream);
    }
}
