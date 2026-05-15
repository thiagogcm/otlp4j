package dev.nthings.otlp4j;

import static dev.nthings.otlp4j.testing.Fixtures.logRecord;
import static dev.nthings.otlp4j.testing.Fixtures.span;
import static dev.nthings.otlp4j.testing.Fixtures.traceData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.connector.CountConnector;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.Processor;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import dev.nthings.otlp4j.processor.Processors;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Unit tests for the pure pipeline layer — processors, connectors, composition and the
/// `ExportResult` value type — exercised entirely in `dev.nthings.otlp4j.model` terms with no
/// transport. This is the reference for example-based unit testing in this project.
class PipelineCompositionTest {

    @Test
    void filterSpansKeepsMatchingSpansAndPrunesEmptyScopes() {
        var captured = new AtomicReference<TraceData>();
        var pipeline = Processors.filterSpans(s -> s.kind() == Span.Kind.SERVER)
                .apply(captureTraces(captured));

        pipeline.consumeTraces(traceData(
                span("internal-op", Span.Kind.INTERNAL),
                span("GET /cart", Span.Kind.SERVER),
                span("GET /checkout", Span.Kind.SERVER)));

        assertThat(captured.get().spans())
                .hasSize(2)
                .allMatch(s -> s.kind() == Span.Kind.SERVER);
    }

    @Test
    void filterSpansPrunesResourcesLeftWithNoSpans() {
        var captured = new AtomicReference<TraceData>();
        var pipeline = Processors.filterSpans(s -> s.kind() == Span.Kind.SERVER)
                .apply(captureTraces(captured));

        pipeline.consumeTraces(traceData(span("internal-only", Span.Kind.INTERNAL)));

        assertThat(captured.get().resourceSpans())
                .as("a resource whose every scope was emptied must be pruned")
                .isEmpty();
    }

    @Test
    void filterLogRecordsKeepsMatchingRecords() {
        var captured = new AtomicReference<LogsData>();
        var pipeline = Processors.filterLogRecords(r -> r.severity() == LogRecord.Severity.ERROR)
                .apply(captureLogs(captured));

        pipeline.consumeLogs(Fixtures.logsData(
                logRecord("debug noise", LogRecord.Severity.DEBUG),
                logRecord("payment failed", LogRecord.Severity.ERROR)));

        assertThat(captured.get().logRecords())
                .hasSize(1)
                .allMatch(r -> r.severity() == LogRecord.Severity.ERROR);
    }

    @Test
    void setResourceAttributeEnrichesEverySignal() {
        var traces = new AtomicReference<TraceData>();
        var logs = new AtomicReference<LogsData>();
        var sink = new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData t) {
                traces.set(t);
                return ExportResult.success();
            }

            @Override
            public ExportResult consumeLogs(LogsData l) {
                logs.set(l);
                return ExportResult.success();
            }
        };
        var pipeline = Processors.setResourceAttribute(
                        "deployment.environment", AttributeValue.of("prod"))
                .apply(sink);

        pipeline.consumeTraces(traceData(span("op", Span.Kind.INTERNAL)));
        pipeline.consumeLogs(Fixtures.logsData(logRecord("hello", LogRecord.Severity.INFO)));

        assertThat(traces.get().resourceSpans().get(0).resource().attributes()
                        .getString("deployment.environment"))
                .isEqualTo("prod");
        assertThat(logs.get().resourceLogs().get(0).resource().attributes()
                        .getString("deployment.environment"))
                .isEqualTo("prod");
    }

    @Test
    void pipelineRunsProcessorsInTheOrderTheyWereAdded() {
        var order = new ArrayList<String>();

        var pipeline = Pipeline.builder()
                .process(recordingProcessor("first", order))
                .process(recordingProcessor("second", order))
                .into(new TelemetryConsumer() {});

        pipeline.consumeTraces(traceData(span("op", Span.Kind.INTERNAL)));

        assertThat(order).containsExactly("first", "second");
    }

    @Test
    void countConnectorDerivesASpanCountMetric() {
        var captured = new AtomicReference<MetricsData>();
        var connector = new CountConnector(new TelemetryConsumer() {
            @Override
            public ExportResult consumeMetrics(MetricsData metrics) {
                captured.set(metrics);
                return ExportResult.success();
            }
        });

        connector.consumeTraces(traceData(
                span("a", Span.Kind.INTERNAL),
                span("b", Span.Kind.SERVER),
                span("c", Span.Kind.CLIENT)));

        var metric = captured.get().metrics().get(0);
        assertThat(metric.name()).isEqualTo("otlp4j.connector.span.count");
        assertThat(metric.data()).isInstanceOf(Metric.Sum.class);
        var value = (NumberPoint.LongValue) ((Metric.Sum) metric.data()).points().get(0).value();
        assertThat(value.value()).isEqualTo(3L);
    }

    // --- ExportResult: validation and merge semantics --------------------------------------

    @Test
    void exportResultRejectsANegativeRejectedCount() {
        assertThatThrownBy(() -> new ExportResult(-1, "bad"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rejectedCount must be >= 0");
    }

    @Test
    void exportResultCoalescesANullMessageToEmpty() {
        assertThat(new ExportResult(0, null).message()).isEmpty();
    }

    @Test
    void exportResultAndCombinesCountsAndJoinsMessages() {
        var combined = ExportResult.partialSuccess(2, "2 spans malformed")
                .and(ExportResult.partialSuccess(3, "3 spans rejected"));

        assertThat(combined.rejectedCount()).isEqualTo(5);
        assertThat(combined.message()).isEqualTo("2 spans malformed; 3 spans rejected");
    }

    @Test
    void exportResultAndIsIdentityOverFullSuccess() {
        var partial = ExportResult.partialSuccess(1, "one rejected");

        assertThat(ExportResult.success().and(partial)).isEqualTo(partial);
        assertThat(partial.and(ExportResult.success())).isEqualTo(partial);
    }

    // --- helpers ---------------------------------------------------------------------------

    private static Processor recordingProcessor(String label, List<String> order) {
        return next -> new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData traces) {
                order.add(label);
                return next.consumeTraces(traces);
            }
        };
    }

    private static TelemetryConsumer captureTraces(AtomicReference<TraceData> sink) {
        return new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData traces) {
                sink.set(traces);
                return ExportResult.success();
            }
        };
    }

    private static TelemetryConsumer captureLogs(AtomicReference<LogsData> sink) {
        return new TelemetryConsumer() {
            @Override
            public ExportResult consumeLogs(LogsData logs) {
                sink.set(logs);
                return ExportResult.success();
            }
        };
    }
}
