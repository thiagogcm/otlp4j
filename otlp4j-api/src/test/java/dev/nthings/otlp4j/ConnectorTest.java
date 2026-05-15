package dev.nthings.otlp4j;

import static dev.nthings.otlp4j.testing.Fixtures.logRecord;
import static dev.nthings.otlp4j.testing.Fixtures.logsData;
import static dev.nthings.otlp4j.testing.Fixtures.metric;
import static dev.nthings.otlp4j.testing.Fixtures.metricsData;
import static dev.nthings.otlp4j.testing.Fixtures.profile;
import static dev.nthings.otlp4j.testing.Fixtures.profilesData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.connector.Connector;
import dev.nthings.otlp4j.connector.CountConnector;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Unit tests for the connector layer — the `Connector` base contract and `CountConnector`'s
/// cross-signal counting, including the intentionally-dropped non-trace/log signals.
class ConnectorTest {

    @Test
    void connectorBaseRejectsANullDownstream() {
        assertThatThrownBy(() -> new CountConnector(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("downstream");
    }

    @Test
    void connectorExposesItsDownstream() {
        var downstream = new TelemetryConsumer() {};
        var captured = new AtomicReference<TelemetryConsumer>();
        var connector = new Connector(downstream) {
            {
                captured.set(downstream());
            }
        };

        assertThat(connector).isNotNull();
        assertThat(captured.get()).isSameAs(downstream);
    }

    @Test
    void countConnectorDerivesALogRecordCountMetric() {
        var captured = new AtomicReference<MetricsData>();
        var connector = new CountConnector(new TelemetryConsumer() {
            @Override
            public ExportResult consumeMetrics(MetricsData metrics) {
                captured.set(metrics);
                return ExportResult.success();
            }
        });

        connector.consumeLogs(logsData(
                logRecord("a", LogRecord.Severity.INFO),
                logRecord("b", LogRecord.Severity.ERROR)));

        var metric = captured.get().metrics().get(0);
        assertThat(metric.name()).isEqualTo("otlp4j.connector.log.record.count");
        assertThat(metric.data()).isInstanceOf(Metric.Sum.class);
        var value = (NumberPoint.LongValue) ((Metric.Sum) metric.data()).points().get(0).value();
        assertThat(value.value()).isEqualTo(2L);
    }

    @Test
    void countConnectorSilentlyDropsMetricsAndProfiles() {
        var downstreamCalls = new AtomicInteger();
        var connector = new CountConnector(new TelemetryConsumer() {
            @Override
            public ExportResult consumeMetrics(MetricsData metrics) {
                downstreamCalls.incrementAndGet();
                return ExportResult.success();
            }
        });

        var metricsResult = connector.consumeMetrics(metricsData(metric("ignored")));
        var profilesResult = connector.consumeProfiles(profilesData(profile("ignored")));

        assertThat(metricsResult.isFullSuccess())
                .as("CountConnector inherits the TelemetryConsumer default: metrics accepted, not forwarded")
                .isTrue();
        assertThat(profilesResult.isFullSuccess())
                .as("CountConnector inherits the TelemetryConsumer default: profiles accepted, not forwarded")
                .isTrue();
        assertThat(downstreamCalls.get())
                .as("neither metrics nor profiles reach the downstream metrics consumer")
                .isZero();
    }
}
