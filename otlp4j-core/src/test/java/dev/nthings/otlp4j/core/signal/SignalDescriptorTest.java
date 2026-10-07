package dev.nthings.otlp4j.core.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.ExponentialHistogramPoint;
import dev.nthings.otlp4j.model.HistogramPoint;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.SummaryPoint;
import dev.nthings.otlp4j.model.TracesData;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Merge and split behavior that preserves telemetry and its resource associations.
class SignalDescriptorTest {

    @Test
    @DisplayName("PROFILES.merge throws IllegalStateException on distinct non-empty dictionaries")
    void profilesMergeParity() {
        var first = new ProfilesData(List.of(), new byte[] {1});
        var second = new ProfilesData(List.of(), new byte[] {2});

        assertThatThrownBy(() -> SignalDescriptor.PROFILES.merge(List.of(first, second)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("distinct ProfilesDictionaries");
    }

    @Test
    @DisplayName("merging retains each resource and its telemetry")
    void mergingRetainsResourceAssociations() {
        var checkout = Resource.of(Attributes.builder().put("service.name", "checkout").build());
        var payments = Resource.of(Attributes.builder().put("service.name", "payments").build());
        var scope = InstrumentationScope.of("library");
        var firstTraces = TracesData.of(checkout, scope, List.of(span("pay", "0102030405060708")));
        var secondTraces = TracesData.of(payments, scope, List.of(span("refund", "0807060504030201")));
        var firstMetrics = MetricsData.of(checkout, scope, List.of(Metric.builder().name("requests").data(Metric.gauge(NumberPoint.of(Attributes.empty(), 10, NumberPoint.longValue(4)))).build()));
        var secondMetrics = MetricsData.of(payments, scope, List.of(Metric.builder().name("refunds").data(Metric.gauge(NumberPoint.of(Attributes.empty(), 10, NumberPoint.longValue(2)))).build()));
        var firstLogs = LogsData.of(checkout, scope, List.of(LogRecord.builder().epochNanos(10).severity(LogRecord.Severity.INFO).body(AttributeValue.of("paid")).build()));
        var secondLogs = LogsData.of(payments, scope, List.of(LogRecord.builder().epochNanos(10).severity(LogRecord.Severity.WARN).body(AttributeValue.of("refunded")).build()));
        var firstProfiles = ProfilesData.of(checkout, scope, List.of(new ProfilesData.Profile("01020304050607080102030405060708", 10, 5, 1, 0, 0, "", new byte[0])));
        var secondProfiles = ProfilesData.of(payments, scope, List.of(new ProfilesData.Profile("08070605040302010807060504030201", 10, 5, 1, 0, 0, "", new byte[0])));

        assertThat(SignalDescriptor.TRACES.merge(List.of(firstTraces, secondTraces)).resourceSpans())
                .containsExactly(firstTraces.resourceSpans().getFirst(), secondTraces.resourceSpans().getFirst());
        assertThat(SignalDescriptor.METRICS.merge(List.of(firstMetrics, secondMetrics)).resourceMetrics())
                .containsExactly(firstMetrics.resourceMetrics().getFirst(), secondMetrics.resourceMetrics().getFirst());
        assertThat(SignalDescriptor.LOGS.merge(List.of(firstLogs, secondLogs)).resourceLogs())
                .containsExactly(firstLogs.resourceLogs().getFirst(), secondLogs.resourceLogs().getFirst());
        assertThat(SignalDescriptor.PROFILES.merge(List.of(firstProfiles, secondProfiles)).resourceProfiles())
                .containsExactly(firstProfiles.resourceProfiles().getFirst(), secondProfiles.resourceProfiles().getFirst());
    }

    @Test
    @DisplayName("metric split handles every metric data variant")
    void metricVariantsSplit() {
        var metrics = MetricsData.of(
                Resource.EMPTY,
                InstrumentationScope.EMPTY,
                List.of(
                        new Metric(
                                "histogram", "", "", new Metric.Histogram(
                                        List.of(HistogramPoint.builder().count(1).build()),
                                        Metric.AggregationTemporality.CUMULATIVE),
                                Attributes.empty()),
                        new Metric(
                                "exponential", "", "", new Metric.ExponentialHistogram(
                                        List.of(ExponentialHistogramPoint.builder().count(1).zeroCount(1).build()),
                                        Metric.AggregationTemporality.CUMULATIVE),
                                Attributes.empty()),
                        new Metric(
                                "summary", "", "", new Metric.Summary(
                                        List.of(SummaryPoint.builder().count(1).build())),
                                Attributes.empty())));

        var parts = SignalDescriptor.METRICS.split(metrics, 1);

        assertThat(parts).hasSize(3);
        assertThat(parts).allSatisfy(part -> assertThat(part.metrics()).hasSize(1));
        assertThat(parts.stream().flatMap(part -> part.metrics().stream()).toList())
                .containsExactlyElementsOf(metrics.metrics());
    }
    private static Span span(String name, String spanId) {
        return Span.builder()
                .traceId("01020304050607080102030405060708")
                .spanId(spanId)
                .name(name)
                .kind(Span.Kind.SERVER)
                .startEpochNanos(10)
                .endEpochNanos(20)
                .build();
    }

}
