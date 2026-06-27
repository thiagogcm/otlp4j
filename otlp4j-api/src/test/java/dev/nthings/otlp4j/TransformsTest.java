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

    @DisplayName("withTracesResourceAttribute adds a resource attribute")
    @Test
    void withTracesResourceAttributeAddsAttribute() {
        var traces = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        var enriched = Transforms.withTracesResourceAttribute("env", AttributeValue.of("prod")).apply(traces);
        var attr = enriched.resourceSpans().getFirst().resource().attributes().get("env");
        assertThat(attr).isInstanceOf(AttributeValue.StringValue.class);
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("prod");
    }

    @DisplayName("withMetricsResourceAttribute adds a resource attribute")
    @Test
    void withMetricsResourceAttributeAddsAttribute() {
        var metrics = Fixtures.metricsData(Fixtures.metric("m1"));
        var enriched = Transforms.withMetricsResourceAttribute("env", AttributeValue.of("dev")).apply(metrics);
        var attr = enriched.resourceMetrics().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("dev");
    }

    @DisplayName("withLogsResourceAttribute adds a resource attribute")
    @Test
    void withLogsResourceAttributeAddsAttribute() {
        var logs = Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO));
        var enriched = Transforms.withLogsResourceAttribute("env", AttributeValue.of("staging")).apply(logs);
        var attr = enriched.resourceLogs().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("staging");
    }

    @DisplayName("withProfilesResourceAttribute adds a resource attribute")
    @Test
    void withProfilesResourceAttributeAddsAttribute() {
        var profiles = Fixtures.profilesData(Fixtures.profile("p1"));
        var enriched = Transforms.withProfilesResourceAttribute("env", AttributeValue.of("perf")).apply(profiles);
        var attr = enriched.resourceProfiles().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("perf");
    }

    @DisplayName("mapSpans rewrites every span without naming wrapper records")
    @Test
    void mapSpansRewritesSpans() {
        var traces = Fixtures.traceData(
                Fixtures.span("a", Span.Kind.SERVER), Fixtures.span("b", Span.Kind.INTERNAL));
        var mapped = Transforms.mapSpans(
                        span -> span.toBuilder().name("redacted").build())
                .apply(traces);
        assertThat(mapped.spans()).extracting(Span::name).containsExactly("redacted", "redacted");
    }

    @DisplayName("mapSpans preserves empty scopes and resources instead of pruning")
    @Test
    void mapSpansPreservesEmptyStructure() {
        var traces = Fixtures.traceData();
        var mapped = Transforms.mapSpans(span -> span).apply(traces);
        assertThat(mapped.resourceSpans()).hasSize(1);
        assertThat(mapped.resourceSpans().getFirst().scopeSpans()).hasSize(1);
        assertThat(mapped.spans()).isEmpty();
    }

    @DisplayName("mapLogRecords rewrites every record without naming wrapper records")
    @Test
    void mapLogRecordsRewritesRecords() {
        var logs = Fixtures.logsData(
                Fixtures.logRecord("secret", LogRecord.Severity.INFO),
                Fixtures.logRecord("other", LogRecord.Severity.WARN));
        var mapped = Transforms.mapLogRecords(
                        record -> record.toBuilder().body(AttributeValue.of("[redacted]")).build())
                .apply(logs);
        assertThat(mapped.logRecords())
                .extracting(LogRecord::body)
                .containsOnly(AttributeValue.of("[redacted]"));
    }

    @DisplayName("mapLogRecords preserves empty scopes and resources instead of pruning")
    @Test
    void mapLogRecordsPreservesEmptyStructure() {
        var logs = Fixtures.logsData();
        var mapped = Transforms.mapLogRecords(record -> record).apply(logs);
        assertThat(mapped.resourceLogs()).hasSize(1);
        assertThat(mapped.resourceLogs().getFirst().scopeLogs()).hasSize(1);
        assertThat(mapped.logRecords()).isEmpty();
    }

    @DisplayName("mapTracesResource rewrites every resource")
    @Test
    void mapTracesResourceRewritesResource() {
        var traces = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        var mapped = Transforms.mapTracesResource(resource -> resource.withAttribute("env", AttributeValue.of("prod")))
                .apply(traces);
        var attr = mapped.resourceSpans().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("prod");
        assertThat(mapped.spans()).hasSize(1);
    }

    @DisplayName("mapMetricsResource rewrites every resource")
    @Test
    void mapMetricsResourceRewritesResource() {
        var metrics = Fixtures.metricsData(Fixtures.metric("m1"));
        var mapped = Transforms.mapMetricsResource(resource -> resource.withAttribute("env", AttributeValue.of("dev")))
                .apply(metrics);
        var attr = mapped.resourceMetrics().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("dev");
        assertThat(mapped.metrics()).hasSize(1);
    }

    @DisplayName("mapLogsResource rewrites every resource")
    @Test
    void mapLogsResourceRewritesResource() {
        var logs = Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO));
        var mapped = Transforms.mapLogsResource(resource -> resource.withAttribute("env", AttributeValue.of("staging")))
                .apply(logs);
        var attr = mapped.resourceLogs().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("staging");
        assertThat(mapped.logRecords()).hasSize(1);
    }

    @DisplayName("mapProfilesResource rewrites every resource and preserves the dictionary")
    @Test
    void mapProfilesResourceRewritesResource() {
        var profiles = Fixtures.profilesData(Fixtures.profile("p1"));
        var mapped = Transforms.mapProfilesResource(resource -> resource.withAttribute("env", AttributeValue.of("perf")))
                .apply(profiles);
        var attr = mapped.resourceProfiles().getFirst().resource().attributes().get("env");
        assertThat(((AttributeValue.StringValue) attr).value()).isEqualTo("perf");
        assertThat(mapped.dictionary()).isEqualTo(profiles.dictionary());
    }
}
