package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for the [Signal] typesafe-enum: its identity metadata (`signalName`, `batchType`,
/// `values`, `toString`), the per-signal `merge`/`itemCount`/`empty` operations folded in from the
/// former `BatchMergers`, and the profiles distinct-dictionary guard.
@DisplayName("Signal")
class SignalTest {

    @DisplayName("values() lists the four signals in wire order")
    @Test
    void valuesInWireOrder() {
        assertThat(Signal.values())
                .containsExactly(Signal.TRACES, Signal.METRICS, Signal.LOGS, Signal.PROFILES);
    }

    @DisplayName("signalName, batchType, and toString identify each signal")
    @Test
    void identityMetadata() {
        assertThat(Signal.TRACES.signalName()).isEqualTo("traces");
        assertThat(Signal.METRICS.signalName()).isEqualTo("metrics");
        assertThat(Signal.LOGS.signalName()).isEqualTo("logs");
        assertThat(Signal.PROFILES.signalName()).isEqualTo("profiles");

        assertThat(Signal.TRACES.batchType()).isEqualTo(TracesData.class);
        assertThat(Signal.METRICS.batchType()).isEqualTo(MetricsData.class);
        assertThat(Signal.LOGS.batchType()).isEqualTo(LogsData.class);
        assertThat(Signal.PROFILES.batchType()).isEqualTo(ProfilesData.class);

        assertThat(Signal.TRACES).hasToString("traces");
    }

    @DisplayName("empty() yields an empty batch of the right type for each signal")
    @Test
    void emptyBatches() {
        assertThat(Signal.TRACES.empty().spanCount()).isZero();
        assertThat(Signal.METRICS.empty().dataPointCount()).isZero();
        assertThat(Signal.LOGS.empty().logRecordCount()).isZero();
        assertThat(Signal.PROFILES.empty().profileCount()).isZero();
    }

    @DisplayName("TRACES merges resource groups and counts spans")
    @Test
    void tracesMergeAndCount() {
        var a = traceData("a");
        var b = traceData("b", "c");
        var merged = Signal.TRACES.merge(List.of(a, b));
        assertThat(merged.spanCount()).isEqualTo(3);
        assertThat(Signal.TRACES.itemCount(merged)).isEqualTo(3L);
    }

    @DisplayName("METRICS concatenates resource groups")
    @Test
    void metricsMerge() {
        var a = metricsData("a");
        var b = metricsData("b");
        assertThat(Signal.METRICS.merge(List.of(a, b)).metrics())
                .extracting(Metric::name)
                .containsExactly("a", "b");
    }

    @DisplayName("LOGS concatenates resource groups and counts records")
    @Test
    void logsMergeAndCount() {
        var a = logsData("a");
        var b = logsData("b");
        var merged = Signal.LOGS.merge(List.of(a, b));
        assertThat(merged.logRecordCount()).isEqualTo(2);
        assertThat(Signal.LOGS.itemCount(merged)).isEqualTo(2L);
    }

    @DisplayName("PROFILES merge preserves a shared dictionary and counts profiles")
    @Test
    void profilesMergeSharedDictionary() {
        var dict = new byte[] {9};
        var a = profilesData(dict, "01");
        var b = profilesData(dict, "02");
        var merged = Signal.PROFILES.merge(List.of(a, b));
        assertThat(merged.profileCount()).isEqualTo(2);
        assertThat(Signal.PROFILES.itemCount(merged)).isEqualTo(2L);
        assertThat(merged.dictionary()).containsExactly(9);
    }

    @DisplayName("PROFILES merge rejects distinct dictionaries")
    @Test
    void profilesMergeRejectsDistinctDictionaries() {
        var a = profilesData(new byte[] {1}, "01");
        var b = profilesData(new byte[] {2}, "02");
        assertThatThrownBy(() -> Signal.PROFILES.merge(List.of(a, b)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ProfilesDictionaries");
    }

    private static TracesData traceData(String... spanNames) {
        var spans = new java.util.ArrayList<Span>();
        for (var name : spanNames) {
            spans.add(Span.builder().name(name).build());
        }
        return TracesData.of(Resource.EMPTY, InstrumentationScope.of("s"), spans);
    }

    private static MetricsData metricsData(String name) {
        var point = new NumberPoint(
                Attributes.empty(), 0L, 2_000L, NumberPoint.longValue(1L), 0L, List.of());
        var metric = Metric.builder().name(name).data(new Metric.Gauge(List.of(point))).build();
        return MetricsData.of(Resource.EMPTY, InstrumentationScope.of("s"), List.of(metric));
    }

    private static LogsData logsData(String eventName) {
        return LogsData.of(
                Resource.EMPTY,
                InstrumentationScope.of("s"),
                List.of(LogRecord.builder().eventName(eventName).build()));
    }

    private static ProfilesData profilesData(byte[] dictionary, String profileId) {
        var profile = new ProfilesData.Profile(profileId, 0L, 0L, 0L, 0, 0, "", new byte[0]);
        return new ProfilesData(
                List.of(new ProfilesData.ResourceProfiles(
                        Resource.EMPTY,
                        "",
                        List.of(new ProfilesData.ScopeProfiles(
                                InstrumentationScope.of("s"), "", List.of(profile))))),
                dictionary);
    }
}
