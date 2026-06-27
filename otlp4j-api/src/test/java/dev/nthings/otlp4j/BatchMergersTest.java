package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.processor.BatchItemCounters;
import dev.nthings.otlp4j.processor.BatchMergers;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Batch mergers and item counters")
class BatchMergersTest {

    @DisplayName("mergeMetrics concatenates resource groups")
    @Test
    void mergeMetrics() {
        var a = Fixtures.metricsData(Fixtures.metric("a"));
        var b = Fixtures.metricsData(Fixtures.metric("b"));
        assertThat(BatchMergers.mergeMetrics(List.of(a, b)).metrics())
                .extracting(dev.nthings.otlp4j.model.Metric::name)
                .containsExactly("a", "b");
    }

    @DisplayName("mergeLogs concatenates resource groups")
    @Test
    void mergeLogs() {
        var a = Fixtures.logsData(Fixtures.logRecord("a", LogRecord.Severity.INFO));
        var b = Fixtures.logsData(Fixtures.logRecord("b", LogRecord.Severity.WARN));
        assertThat(BatchMergers.mergeLogs(List.of(a, b)).logRecordCount()).isEqualTo(2);
    }

    @DisplayName("mergeProfilesUnsafe preserves a shared dictionary")
    @Test
    void mergeProfilesSharedDictionary() {
        var dict = new byte[] { 9 };
        var a = new ProfilesData(Fixtures.profilesData(Fixtures.profile("01")).resourceProfiles(), dict);
        var b = new ProfilesData(Fixtures.profilesData(Fixtures.profile("02")).resourceProfiles(), dict);
        assertThat(BatchMergers.mergeProfilesUnsafe(List.of(a, b)).profileCount()).isEqualTo(2);
    }

    @DisplayName("BatchItemCounters delegates to MetricsData.dataPointCount")
    @Test
    void metricDataPoints() {
        var metrics = Fixtures.metricsData(Fixtures.metric("one"));
        assertThat(BatchItemCounters.metricDataPoints(metrics)).isEqualTo(metrics.dataPointCount());
    }
}
