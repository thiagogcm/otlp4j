package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.processor.BatchMergers;
import dev.nthings.otlp4j.processor.Signal;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Signal and batch mergers")
class SignalTest {

    @DisplayName("Signal.TRACES merges resource groups")
    @Test
    void tracesMerge() {
        var a = Fixtures.traceData(Fixtures.span("a", dev.nthings.otlp4j.model.Span.Kind.SERVER));
        var b = Fixtures.traceData(Fixtures.span("b", dev.nthings.otlp4j.model.Span.Kind.SERVER));
        var merged = Signal.TRACES.merge(List.of(a, b));
        assertThat(merged.spanCount()).isEqualTo(2);
        assertThat(BatchMergers.mergeTraces(List.of(a, b))).isEqualTo(merged);
    }

    @DisplayName("Signal.TRACES counts spans")
    @Test
    void tracesItemCount() {
        var batch = Fixtures.traceData(
                Fixtures.span("a", dev.nthings.otlp4j.model.Span.Kind.SERVER),
                Fixtures.span("b", dev.nthings.otlp4j.model.Span.Kind.SERVER));
        assertThat(Signal.TRACES.itemCount(batch)).isEqualTo(2L);
    }

    @DisplayName("Signal.TRACES wraps Telemetry envelope")
    @Test
    void tracesEnvelope() {
        var batch = new TraceData(List.of());
        assertThat(Signal.TRACES.envelope(batch)).isInstanceOf(dev.nthings.otlp4j.core.Telemetry.Traces.class);
    }

    @DisplayName("profiles unsafe merge rejects distinct dictionaries")
    @Test
    void profilesUnsafeMergeRejectsDistinctDictionaries() {
        var a = Fixtures.profilesDataWithDictionary(new byte[] { 1 }, Fixtures.profile("01"));
        var b = Fixtures.profilesDataWithDictionary(new byte[] { 2 }, Fixtures.profile("02"));
        assertThatThrownBy(() -> Signal.PROFILES.merge(List.of(a, b)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ProfilesDictionaries");
    }
}
