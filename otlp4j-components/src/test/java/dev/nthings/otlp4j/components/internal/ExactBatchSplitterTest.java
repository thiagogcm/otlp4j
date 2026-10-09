package dev.nthings.otlp4j.components.internal;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.PayloadSizer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.testing.Fixtures;
import org.junit.jupiter.api.Test;

class ExactBatchSplitterTest {

    /// Ten bytes for the request and its grouping plus one hundred per item, so a unit's weight is not
    /// the sum of its items' weights.
    private static final PayloadSizer SIZER = payload -> 10L + 100L * payload.itemCount();

    @Test
    void zeroCapsPreserveOnePayloadForEverySignal() {
        assertThat(ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(3).data(), 0, 0, SIZER))
                .singleElement()
                .extracting(Payload::itemCount)
                .isEqualTo(3L);

        var metrics = Fixtures.metricsData(Fixtures.metric("requests"), Fixtures.metric("errors"));
        assertThat(ExactBatchSplitter.split(SignalDescriptor.METRICS, metrics, 0, 0, SIZER))
                .singleElement()
                .extracting(Payload::itemCount)
                .isEqualTo(2L);

        var logs = Fixtures.logsData(
                Fixtures.logRecord("started", LogRecord.Severity.INFO),
                Fixtures.logRecord("finished", LogRecord.Severity.INFO));
        assertThat(ExactBatchSplitter.split(SignalDescriptor.LOGS, logs, 0, 0, SIZER))
                .singleElement()
                .extracting(Payload::itemCount)
                .isEqualTo(2L);
    }

    @Test
    void itemCapSplitsAllSignalsInOrder() {
        var traces = ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(5).data(), 2, 0, SIZER);
        assertThat(traces).extracting(Payload::itemCount).containsExactly(2L, 2L, 1L);
        assertThat(traces.stream().flatMap(unit -> unit.data().spans().stream()).map(Span::name))
                .containsExactly("span-0", "span-1", "span-2", "span-3", "span-4");

        var metrics = Fixtures.metricsData(
                Fixtures.metric("one"), Fixtures.metric("two"), Fixtures.metric("three"), Fixtures.metric("four"));
        var metricParts = ExactBatchSplitter.split(SignalDescriptor.METRICS, metrics, 2, 0, SIZER);
        assertThat(metricParts.stream().flatMap(unit -> unit.data().metrics().stream()).map(Metric::name))
                .containsExactly("one", "two", "three", "four");
        assertThat(metricParts)
                .extracting(Payload::itemCount)
                .containsExactly(2L, 2L);

        var logs = Fixtures.logsData(
                Fixtures.logRecord("one", LogRecord.Severity.INFO),
                Fixtures.logRecord("two", LogRecord.Severity.INFO),
                Fixtures.logRecord("three", LogRecord.Severity.INFO));
        var logParts = ExactBatchSplitter.split(SignalDescriptor.LOGS, logs, 2, 0, SIZER);
        assertThat(logParts.stream().flatMap(unit -> unit.data().logRecords().stream()).map(LogRecord::body))
                .containsExactly(logs.logRecords().get(0).body(), logs.logRecords().get(1).body(), logs.logRecords().get(2).body());
        assertThat(logParts)
                .extracting(Payload::itemCount)
                .containsExactly(2L, 1L);
    }

    @Test
    void byteCapSplitsUntilEachUnitFitsIncludingItsGroupOverhead() {
        // Two items weigh 210, so a 200-byte cap leaves one item per unit although 2 x 100 would fit.
        var units = ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(5).data(), 0, 200, SIZER);

        assertThat(units).extracting(Payload::itemCount).containsExactly(1L, 1L, 1L, 1L, 1L);
        assertThat(units.stream().flatMap(unit -> unit.data().spans().stream()).map(Span::name))
                .containsExactly("span-0", "span-1", "span-2", "span-3", "span-4");
    }

    @Test
    void byteCapMaySplitBelowItemCapForMetricsAndLogs() {
        var metrics = Fixtures.metricsData(Fixtures.metric("requests"), Fixtures.metric("errors"));
        var logs = Fixtures.logsData(
                Fixtures.logRecord("started", LogRecord.Severity.INFO),
                Fixtures.logRecord("finished", LogRecord.Severity.INFO));

        assertThat(ExactBatchSplitter.split(SignalDescriptor.METRICS, metrics, 10, 110, SIZER))
                .extracting(Payload::itemCount)
                .containsExactly(1L, 1L);
        assertThat(ExactBatchSplitter.split(SignalDescriptor.LOGS, logs, 10, 110, SIZER))
                .extracting(Payload::itemCount)
                .containsExactly(1L, 1L);
    }

    @Test
    void rejectsIndivisibleItemThatExceedsByteCap() {
        assertThatThrownBy(() -> ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(1).data(), 0, 109, SIZER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("indivisible");
    }

    @Test
    void aByteCapRejectsAUnitWithItemsThatTheSizerDoesNotWeigh() {
        PayloadSizer unable = payload -> 0;

        assertThatThrownBy(() -> ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(2).data(), 0, 100, unable))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("byte cap");
        // Without a byte cap the sizer is not consulted, and an empty unit has nothing to weigh.
        assertThat(ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(2).data(), 0, 0, unable)).hasSize(1);
        assertThat(ExactBatchSplitter.split(SignalDescriptor.TRACES, SignalDescriptor.TRACES.empty(), 0, 100, unable))
                .hasSize(1);
    }

    @Test
    void rejectsNegativeCaps() {
        assertThatThrownBy(() -> ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(1).data(), -1, 0, SIZER))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(1).data(), 0, -1, SIZER))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
