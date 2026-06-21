package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.processor.Transforms;
import dev.nthings.otlp4j.testing.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Transforms")
class TransformsTest {

    @DisplayName("keepSpansWhere filters spans and prunes empty scopes")
    @Test
    void keepSpansWhereFiltersAndPrunes() {
        var traces = Fixtures.traceData(
                Fixtures.span("a", Span.Kind.SERVER),
                Fixtures.span("b", Span.Kind.INTERNAL),
                Fixtures.span("c", Span.Kind.SERVER));
        var filtered = Transforms.keepSpansWhere(s -> s.kind() == Span.Kind.SERVER).apply(traces);
        assertThat(filtered.spans()).hasSize(2);
    }

    @DisplayName("keepSpansWhere yields empty data when nothing matches")
    @Test
    void keepSpansWhereResultsInEmptyDataWhenNothingMatches() {
        var traces = Fixtures.traceData(Fixtures.span("a", Span.Kind.INTERNAL));
        var filtered = Transforms.keepSpansWhere(s -> s.kind() == Span.Kind.SERVER).apply(traces);
        assertThat(filtered.spans()).isEmpty();
        assertThat(filtered.resourceSpans()).isEmpty();
    }

    @DisplayName("keepLogRecordsWhere filters records by severity")
    @Test
    void keepLogRecordsFiltersAndPrunes() {
        var logs = Fixtures.logsData(
                Fixtures.logRecord("info", LogRecord.Severity.INFO),
                Fixtures.logRecord("error", LogRecord.Severity.ERROR));
        var filtered = Transforms.keepLogRecordsWhere(r -> r.severity() == LogRecord.Severity.ERROR).apply(logs);
        assertThat(filtered.logRecords()).hasSize(1);
        assertThat(filtered.logRecords().getFirst().body()).isEqualTo(AttributeValue.of("error"));
    }

    @DisplayName("setTraceResourceAttribute adds a resource attribute")
    @Test
    void setTraceResourceAttributeAddsAttribute() {
        var traces = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        var enriched = Transforms.setTraceResourceAttribute("env", AttributeValue.of("prod")).apply(traces);
        var attr = enriched.resourceSpans().getFirst().resource().attributes().get("env");
        assertThat(attr).isInstanceOf(AttributeValue.StringValue.class);
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("prod");
    }

    @DisplayName("setMetricsResourceAttribute adds a resource attribute")
    @Test
    void setMetricsResourceAttributeAddsAttribute() {
        var metrics = Fixtures.metricsData(Fixtures.metric("m1"));
        var enriched = Transforms.setMetricsResourceAttribute("env", AttributeValue.of("dev")).apply(metrics);
        var attr = enriched.resourceMetrics().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("dev");
    }

    @DisplayName("setLogsResourceAttribute adds a resource attribute")
    @Test
    void setLogsResourceAttributeAddsAttribute() {
        var logs = Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO));
        var enriched = Transforms.setLogsResourceAttribute("env", AttributeValue.of("staging")).apply(logs);
        var attr = enriched.resourceLogs().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("staging");
    }

    @DisplayName("setProfilesResourceAttribute adds a resource attribute")
    @Test
    void setProfilesResourceAttributeAddsAttribute() {
        var profiles = Fixtures.profilesData(Fixtures.profile("p1"));
        var enriched = Transforms.setProfilesResourceAttribute("env", AttributeValue.of("perf")).apply(profiles);
        var attr = enriched.resourceProfiles().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("perf");
    }
}
