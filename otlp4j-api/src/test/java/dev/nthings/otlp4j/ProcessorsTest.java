package dev.nthings.otlp4j;

import static dev.nthings.otlp4j.testing.Fixtures.logRecord;
import static dev.nthings.otlp4j.testing.Fixtures.logsData;
import static dev.nthings.otlp4j.testing.Fixtures.metric;
import static dev.nthings.otlp4j.testing.Fixtures.metricsData;
import static dev.nthings.otlp4j.testing.Fixtures.profile;
import static dev.nthings.otlp4j.testing.Fixtures.profilesData;
import static dev.nthings.otlp4j.testing.Fixtures.span;
import static dev.nthings.otlp4j.testing.Fixtures.traceData;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import dev.nthings.otlp4j.processor.Processors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Unit tests for `Processors` — the filtering and enrichment transforms — covering every
/// signal arm and both the keep and prune branches of the filters.
class ProcessorsTest {

    @Test
    void filterLogRecordsKeepsMatchingRecordsAndPrunesEmptyScopes() {
        var captured = new AtomicReference<LogsData>();
        var pipeline = Processors.filterLogRecords(r -> r.severity() == LogRecord.Severity.ERROR)
                .apply(captureLogs(captured));

        pipeline.consumeLogs(logsData(
                logRecord("debug noise", LogRecord.Severity.DEBUG),
                logRecord("payment failed", LogRecord.Severity.ERROR)));

        assertThat(captured.get().logRecords())
                .hasSize(1)
                .allMatch(r -> r.severity() == LogRecord.Severity.ERROR);
    }

    @Test
    void filterLogRecordsPrunesResourcesLeftWithNoRecords() {
        var captured = new AtomicReference<LogsData>();
        var pipeline = Processors.filterLogRecords(r -> r.severity() == LogRecord.Severity.ERROR)
                .apply(captureLogs(captured));

        pipeline.consumeLogs(logsData(logRecord("debug only", LogRecord.Severity.DEBUG)));

        assertThat(captured.get().resourceLogs())
                .as("a resource whose every scope was emptied must be pruned")
                .isEmpty();
    }

    @Test
    void filterSpansPrunesEmptyScopesButKeepsNonEmptyOnes() {
        var captured = new AtomicReference<TraceData>();
        var pipeline = Processors.filterSpans(s -> s.kind() == Span.Kind.SERVER)
                .apply(captureTraces(captured));

        pipeline.consumeTraces(traceData(
                span("internal-op", Span.Kind.INTERNAL),
                span("GET /cart", Span.Kind.SERVER)));

        assertThat(captured.get().resourceSpans()).hasSize(1);
        assertThat(captured.get().spans()).hasSize(1).allMatch(s -> s.kind() == Span.Kind.SERVER);
    }

    @Test
    void filterLogRecordsPassesOtherSignalsThroughUntouched() {
        var traces = new AtomicReference<TraceData>();
        var pipeline = Processors.filterLogRecords(r -> false).apply(new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData t) {
                traces.set(t);
                return ExportResult.success();
            }
        });

        pipeline.consumeTraces(traceData(span("op", Span.Kind.INTERNAL)));

        assertThat(traces.get().spans()).as("traces flow downstream untouched").hasSize(1);
    }

    @Test
    void setResourceAttributeEnrichesMetrics() {
        var captured = new AtomicReference<MetricsData>();
        var pipeline = Processors.setResourceAttribute("region", AttributeValue.of("eu-west-1"))
                .apply(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeMetrics(MetricsData m) {
                        captured.set(m);
                        return ExportResult.success();
                    }
                });

        pipeline.consumeMetrics(metricsData(metric("requests")));

        assertThat(captured.get().resourceMetrics().get(0).resource().attributes().getString("region"))
                .isEqualTo("eu-west-1");
    }

    @Test
    void setResourceAttributeEnrichesProfiles() {
        var captured = new AtomicReference<ProfilesData>();
        var pipeline = Processors.setResourceAttribute("region", AttributeValue.of("eu-west-1"))
                .apply(new TelemetryConsumer() {
                    @Override
                    public ExportResult consumeProfiles(ProfilesData p) {
                        captured.set(p);
                        return ExportResult.success();
                    }
                });

        pipeline.consumeProfiles(profilesData(profile("abc")));

        assertThat(captured.get()
                        .resourceProfiles()
                        .get(0)
                        .resource()
                        .attributes()
                        .getString("region"))
                .isEqualTo("eu-west-1");
    }

    @Test
    void setResourceAttributeOverwritesAnExistingValue() {
        var captured = new AtomicReference<TraceData>();
        var pipeline = Processors.setResourceAttribute("service.name", AttributeValue.of("renamed"))
                .apply(captureTraces(captured));

        pipeline.consumeTraces(traceData(span("op", Span.Kind.INTERNAL)));

        assertThat(captured.get().resourceSpans().get(0).resource().attributes().getString("service.name"))
                .as("an existing resource attribute is overwritten")
                .isEqualTo("renamed");
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
