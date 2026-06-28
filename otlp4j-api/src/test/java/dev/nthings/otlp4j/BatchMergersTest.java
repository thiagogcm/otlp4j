package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.processor.BatchMergers;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Batch mergers")
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
        var a = Fixtures.profilesDataWithDictionary(dict, Fixtures.profile("01"));
        var b = Fixtures.profilesDataWithDictionary(dict, Fixtures.profile("02"));
        assertThat(BatchMergers.mergeProfilesUnsafe(List.of(a, b)).profileCount()).isEqualTo(2);
    }
}
