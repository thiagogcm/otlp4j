package dev.nthings.otlp4j.transport.internal;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.RecordingGatewayMetrics;
import dev.nthings.otlp4j.testing.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Pins the exporter counters owned by [ExporterMetrics]: `otlp4j.exporter.export.success` on an
/// acknowledged batch, `otlp4j.exporter.export.failure` on a Rejected result or a failed stage, plus
/// the item, byte, latency, and retry dimensions. Drives [ExporterMetrics#recordSend] and
/// [ExporterMetrics#recordRetry] directly, the single boundary through which the exporter facet writes.
@DisplayName("Exporter self-observability")
class ExporterMetricsTest {

    private static final String SUCCESS = "otlp4j.exporter.export.success";
    private static final String FAILURE = "otlp4j.exporter.export.failure";
    private static final String ITEMS = "otlp4j.exporter.export.items";
    private static final String BYTES = "otlp4j.exporter.export.bytes";
    private static final String DURATION = "otlp4j.exporter.export.duration.nanos";
    private static final String RETRIES = "otlp4j.exporter.export.retries";

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("otlp"), ComponentKind.EXPORTER, TRACES);

    private static Payload<TracesData> tracePayload() {
        return Payload.of(TRACES, Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)));
    }

    @DisplayName("an acknowledged export increments the success counter")
    @Test
    void acknowledgedExportCountsSuccess() {
        var registry = new RecordingGatewayMetrics();
        ExporterMetrics.from(registry).recordSend(
                tracePayload(), 42L, 1_000L, DeliveryResult.accepted(PROV), null);
        assertThat(registry.counterValue(SUCCESS)).isEqualTo(1);
        assertThat(registry.counterValue(FAILURE)).isZero();
    }

    @DisplayName("a Rejected result increments the failure counter")
    @Test
    void rejectedResultCountsFailure() {
        var registry = new RecordingGatewayMetrics();
        ExporterMetrics.from(registry).recordSend(
                tracePayload(), 42L, 1_000L, DeliveryResult.retryable("endpoint at capacity", PROV), null);
        assertThat(registry.counterValue(FAILURE)).isEqualTo(1);
        assertThat(registry.counterValue(SUCCESS)).isZero();
    }

    @DisplayName("a failed stage increments the failure counter")
    @Test
    void failedStageCountsFailure() {
        var registry = new RecordingGatewayMetrics();
        ExporterMetrics.from(registry).recordSend(
                tracePayload(), 42L, 1_000L, null, new RuntimeException("boom"));
        assertThat(registry.counterValue(FAILURE)).isEqualTo(1);
        assertThat(registry.counterValue(SUCCESS)).isZero();
    }

    @DisplayName("a send records the payload's item count, byte size, and accumulated latency")
    @Test
    void sendRecordsItemsBytesAndLatency() {
        var registry = new RecordingGatewayMetrics();
        var payload = tracePayload();
        var expectedItems = payload.itemCount();
        var expectedBytes = 42L;
        ExporterMetrics.from(registry).recordSend(
                payload, expectedBytes, 4_242L, DeliveryResult.accepted(PROV), null);
        assertThat(registry.counterValue(ITEMS)).isEqualTo(expectedItems);
        assertThat(registry.counterValue(BYTES)).isEqualTo(expectedBytes);
        assertThat(registry.counterValue(DURATION)).isEqualTo(4_242L);
    }

    @DisplayName("recordRetry increments the retry counter once per call")
    @Test
    void recordRetryCountsEachAttempt() {
        var registry = new RecordingGatewayMetrics();
        var metrics = ExporterMetrics.from(registry);
        metrics.recordRetry();
        metrics.recordRetry();
        metrics.recordRetry();
        assertThat(registry.counterValue(RETRIES)).isEqualTo(3);
    }

    private static final String QUEUE_SIZE = "otlp4j.exporter.queue.size";

    @DisplayName("queue refusals and losses are counted in items")
    @Test
    void queueRefusalsAndLossesAreCounted() {
        var registry = new RecordingGatewayMetrics();
        var metrics = ExporterMetrics.from(registry);

        metrics.recordQueueRefused(4);
        metrics.recordQueueRefused(1);
        metrics.recordQueueLost(7);

        assertThat(registry.counterValue("otlp4j.exporter.queue.refused.items")).isEqualTo(5);
        assertThat(registry.counterValue("otlp4j.exporter.queue.lost.items")).isEqualTo(7);
    }

    @DisplayName("the queues of one registry share one depth gauge that sums them and outlives all but the last")
    @Test
    void queueDepthGaugeSumsAcrossQueuesWithoutClobbering() {
        var registry = new RecordingGatewayMetrics();
        // Two exporters on one registry, each with queues of its own: a gauge is keyed by name alone,
        // so a registration per queue would let the last replace the others.
        var first = ExporterMetrics.from(registry);
        var second = ExporterMetrics.from(registry);
        var traces = new java.util.concurrent.atomic.AtomicLong(3);
        var metricsQueue = new java.util.concurrent.atomic.AtomicLong(4);
        var logs = new java.util.concurrent.atomic.AtomicLong(5);

        var a = first.registerQueueSize(traces::get);
        var b = first.registerQueueSize(metricsQueue::get);
        var c = second.registerQueueSize(logs::get);
        assertThat(registry.gaugeValue(QUEUE_SIZE)).isEqualTo(12);

        traces.set(0);
        assertThat(registry.gaugeValue(QUEUE_SIZE)).isEqualTo(9);

        a.close();
        a.close();
        assertThat(registry.gaugeValue(QUEUE_SIZE)).isEqualTo(9);
        b.close();
        assertThat(registry.gaugeValue(QUEUE_SIZE)).isEqualTo(5);
        c.close();
        assertThat(registry.gaugeValue(QUEUE_SIZE)).isZero();

        // A queue that starts after the gauge went away registers it afresh.
        var d = first.registerQueueSize(() -> 2);
        assertThat(registry.gaugeValue(QUEUE_SIZE)).isEqualTo(2);
        d.close();
    }

    @DisplayName("the depth gauge is not registered when self-observability is off")
    @Test
    void queueDepthGaugeIsInertUnderNoop() {
        ExporterMetrics.noop().registerQueueSize(() -> 1).close();
        ExporterMetrics.from(dev.nthings.otlp4j.core.telemetry.GatewayMetrics.noop()).recordQueueLost(3);
    }
}
