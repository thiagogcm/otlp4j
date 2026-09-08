package dev.nthings.otlp4j.components.internal;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.codec.PayloadWire;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import org.junit.jupiter.api.Test;

class ExactBatchSplitterTest {

    @Test
    void zeroCapsPreserveOnePayloadForEverySignal() {
        assertThat(ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(3).data(), 0, 0))
                .singleElement()
                .extracting(Payload::itemCount)
                .isEqualTo(3L);

        var metrics = Fixtures.metricsData(Fixtures.metric("requests"), Fixtures.metric("errors"));
        assertThat(ExactBatchSplitter.split(SignalDescriptor.METRICS, metrics, 0, 0))
                .singleElement()
                .extracting(Payload::itemCount)
                .isEqualTo(2L);

        var logs = Fixtures.logsData(
                Fixtures.logRecord("started", LogRecord.Severity.INFO),
                Fixtures.logRecord("finished", LogRecord.Severity.INFO));
        assertThat(ExactBatchSplitter.split(SignalDescriptor.LOGS, logs, 0, 0))
                .singleElement()
                .extracting(Payload::itemCount)
                .isEqualTo(2L);
    }

    @Test
    void itemCapSplitsAllSignalsInOrder() {
        var traces = ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(5).data(), 2, 0);
        assertThat(traces).extracting(Payload::itemCount).containsExactly(2L, 2L, 1L);
        assertThat(traces).allSatisfy(unit -> assertThat(unit.data()).isInstanceOf(TracesData.class));

        var metrics = Fixtures.metricsData(
                Fixtures.metric("one"), Fixtures.metric("two"), Fixtures.metric("three"), Fixtures.metric("four"));
        assertThat(ExactBatchSplitter.split(SignalDescriptor.METRICS, metrics, 2, 0))
                .extracting(Payload::itemCount)
                .containsExactly(2L, 2L);

        var logs = Fixtures.logsData(
                Fixtures.logRecord("one", LogRecord.Severity.INFO),
                Fixtures.logRecord("two", LogRecord.Severity.INFO),
                Fixtures.logRecord("three", LogRecord.Severity.INFO));
        assertThat(ExactBatchSplitter.split(SignalDescriptor.LOGS, logs, 2, 0))
                .extracting(Payload::itemCount)
                .containsExactly(2L, 1L);
    }

    @Test
    void byteCapUsesExactWireSizeAndIncludesGroupOverhead() {
        var data = traces(5).data();
        var oneItem = ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(1).data(), 0, 0).get(0);
        var oneItemBytes = PayloadWire.size(oneItem);
        var units = ExactBatchSplitter.split(SignalDescriptor.TRACES, data, 0, oneItemBytes);

        assertThat(units).extracting(Payload::itemCount).containsExactly(1L, 1L, 1L, 1L, 1L);
        assertThat(units).allSatisfy(unit -> assertThat(PayloadWire.size(unit)).isLessThanOrEqualTo(oneItemBytes));
    }

    @Test
    void byteCapMaySplitBelowItemCapForMetricsAndLogs() {
        var metrics = Fixtures.metricsData(Fixtures.metric("requests"), Fixtures.metric("errors"));
        var logs = Fixtures.logsData(
                Fixtures.logRecord("started", LogRecord.Severity.INFO),
                Fixtures.logRecord("finished", LogRecord.Severity.INFO));

        var metricBytes = PayloadWire.size(Payload.of(
                SignalDescriptor.METRICS, Fixtures.metricsData(Fixtures.metric("requests"))));
        var logBytes = Math.max(
                PayloadWire.size(Payload.of(SignalDescriptor.LOGS,
                        Fixtures.logsData(Fixtures.logRecord("started", LogRecord.Severity.INFO)))),
                PayloadWire.size(Payload.of(SignalDescriptor.LOGS,
                        Fixtures.logsData(Fixtures.logRecord("finished", LogRecord.Severity.INFO)))));

        assertThat(ExactBatchSplitter.split(SignalDescriptor.METRICS, metrics, 10, metricBytes))
                .extracting(Payload::itemCount)
                .containsExactly(1L, 1L);
        assertThat(ExactBatchSplitter.split(SignalDescriptor.LOGS, logs, 10, logBytes))
                .extracting(Payload::itemCount)
                .containsExactly(1L, 1L);
    }

    @Test
    void rejectsIndivisibleItemThatExceedsByteCap() {
        var item = PayloadWire.size(Payload.of(SignalDescriptor.TRACES, traces(1).data()));
        assertThatThrownBy(() -> ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(1).data(), 0, item - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("indivisible");
    }

    @Test
    void rejectsNegativeCaps() {
        assertThatThrownBy(() -> ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(1).data(), -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExactBatchSplitter.split(SignalDescriptor.TRACES, traces(1).data(), 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
