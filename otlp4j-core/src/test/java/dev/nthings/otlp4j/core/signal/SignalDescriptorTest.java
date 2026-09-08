package dev.nthings.otlp4j.core.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.ExponentialHistogramPoint;
import dev.nthings.otlp4j.model.HistogramPoint;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.SummaryPoint;
import dev.nthings.otlp4j.model.TracesData;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [SignalDescriptor]: the closed value set, its wire ordering, and the
/// merge/itemCount/empty operations it owns directly over the immutable model records. The
/// profiles-merge test is the guard the report requires: merging batches that carry distinct
/// non-empty dictionaries throws.
class SignalDescriptorTest {

    @Test
    @DisplayName("values are the four signals in wire order, each with the right ordinal and name")
    void valuesInWireOrder() {
        assertThat(SignalDescriptor.values())
                .containsExactly(
                        SignalDescriptor.TRACES,
                        SignalDescriptor.METRICS,
                        SignalDescriptor.LOGS,
                        SignalDescriptor.PROFILES);

        assertThat(SignalDescriptor.TRACES.wireOrdinal()).isZero();
        assertThat(SignalDescriptor.METRICS.wireOrdinal()).isEqualTo(1);
        assertThat(SignalDescriptor.LOGS.wireOrdinal()).isEqualTo(2);
        assertThat(SignalDescriptor.PROFILES.wireOrdinal()).isEqualTo(3);

        assertThat(SignalDescriptor.TRACES.signalName()).isEqualTo("traces");
        assertThat(SignalDescriptor.TRACES).hasToString("traces");
        assertThat(SignalDescriptor.METRICS.signalName()).isEqualTo("metrics");
        assertThat(SignalDescriptor.LOGS.signalName()).isEqualTo("logs");
        assertThat(SignalDescriptor.PROFILES.signalName()).isEqualTo("profiles");
    }

    @Test
    @DisplayName("batchType matches the model batch class for each signal")
    void batchTypes() {
        assertThat(SignalDescriptor.TRACES.batchType()).isEqualTo(TracesData.class);
        assertThat(SignalDescriptor.METRICS.batchType()).isEqualTo(MetricsData.class);
        assertThat(SignalDescriptor.LOGS.batchType()).isEqualTo(LogsData.class);
        assertThat(SignalDescriptor.PROFILES.batchType()).isEqualTo(ProfilesData.class);
    }

    @Test
    @DisplayName("empty, merge and itemCount compose correctly over TracesData")
    void mergeAndItemCount() {
        assertThat(SignalDescriptor.TRACES.empty().spanCount()).isZero();

        var one = new TracesData(new TracesData(List.of()).resourceSpans());
        var merged = SignalDescriptor.TRACES.merge(List.of(one, SignalDescriptor.TRACES.empty()));
        assertThat(SignalDescriptor.TRACES.itemCount(merged)).isEqualTo(one.spanCount());
    }

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
    @DisplayName("merge and routing metadata cover every signal descriptor")
    void allSignalOperations() {
        var resource = Resource.of(Attributes.builder().put("service.name", "checkout").build());
        var scope = InstrumentationScope.of("library");
        var traces = TracesData.of(resource, scope, List.of());
        var metrics = MetricsData.of(resource, scope, List.of());
        var logs = LogsData.of(resource, scope, List.of());
        var profiles = ProfilesData.of(resource, scope, List.of());

        assertThat(SignalDescriptor.TRACES.merge(List.of(traces, traces)).resourceSpans())
                .hasSize(2);
        assertThat(SignalDescriptor.METRICS.merge(List.of(metrics, metrics)).resourceMetrics())
                .hasSize(2);
        assertThat(SignalDescriptor.LOGS.merge(List.of(logs, logs)).resourceLogs()).hasSize(2);
        assertThat(SignalDescriptor.PROFILES.merge(List.of(profiles, profiles)).resourceProfiles())
                .hasSize(2);

        assertThat(Payload.of(SignalDescriptor.TRACES, traces).routingMetadata().resourceAttribute("service.name"))
                .contains("checkout");
        assertThat(Payload.of(SignalDescriptor.METRICS, metrics).routingMetadata().resourceAttribute("service.name"))
                .contains("checkout");
        assertThat(Payload.of(SignalDescriptor.LOGS, logs).routingMetadata().resourceAttribute("service.name"))
                .contains("checkout");
        assertThat(Payload.of(SignalDescriptor.PROFILES, profiles).routingMetadata().resourceAttribute("service.name"))
                .contains("checkout");
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
                                        List.of(ExponentialHistogramPoint.builder().count(1).build()),
                                        Metric.AggregationTemporality.CUMULATIVE),
                                Attributes.empty()),
                        new Metric(
                                "summary", "", "", new Metric.Summary(
                                        List.of(SummaryPoint.builder().count(1).build())),
                                Attributes.empty())));

        var parts = SignalDescriptor.METRICS.split(metrics, 1);

        assertThat(parts).hasSize(3);
        assertThat(parts.stream().mapToLong(SignalDescriptor.METRICS::itemCount).sum()).isEqualTo(3);
        assertThat(parts).allSatisfy(part -> assertThat(part.metrics()).hasSize(1));
    }
}
