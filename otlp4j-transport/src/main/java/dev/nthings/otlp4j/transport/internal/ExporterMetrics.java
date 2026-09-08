package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import org.jspecify.annotations.Nullable;

/// The single owner of the exporter's self-observability instruments. One instance is built per
/// exporter from a [GatewayMetrics] registry and is the only place each `otlp4j.exporter.*` counter is
/// written, so the outcome switch and the item, byte, latency, and retry accounting live in exactly one
/// place. The exporter builder constructs one instance and hands the same object to both the wire
/// client (which alone can observe a retry attempt, since that happens inside the retry loop) and the
/// [OtlpExporter] send boundary (which records everything visible when a send settles).
///
/// Latency is accumulated as a nanosecond counter rather than a histogram because [GatewayMetrics]
/// exposes only counters and gauges. Paired with the success and failure counts it yields an average
/// send latency. A real latency histogram is a known limitation of the current self-observability SPI.
///
/// Instances are cheap and hold only counter handles. [GatewayMetrics] implementations are required to
/// be thread-safe, so a single instance is safely shared across the export virtual threads that write
/// the retry counter and the boundary that writes the rest.
public final class ExporterMetrics {

    private static final ExporterMetrics NOOP = new ExporterMetrics(GatewayMetrics.noop());

    private final GatewayMetrics.LongCounter success;
    private final GatewayMetrics.LongCounter failure;
    private final GatewayMetrics.LongCounter items;
    private final GatewayMetrics.LongCounter bytes;
    private final GatewayMetrics.LongCounter durationNanos;
    private final GatewayMetrics.LongCounter retries;

    private ExporterMetrics(GatewayMetrics telemetry) {
        this.success = telemetry.counter(
                "otlp4j.exporter.export.success", "batches the endpoint acknowledged");
        this.failure = telemetry.counter(
                "otlp4j.exporter.export.failure", "batches the endpoint rejected or that failed to send");
        this.items = telemetry.counter(
                "otlp4j.exporter.export.items", "OTLP items sent across all exports");
        this.bytes = telemetry.counter(
                "otlp4j.exporter.export.bytes", "serialized request bytes sent across all exports");
        this.durationNanos = telemetry.counter(
                "otlp4j.exporter.export.duration.nanos", "accumulated send latency in nanoseconds");
        this.retries = telemetry.counter(
                "otlp4j.exporter.export.retries", "retry attempts beyond the first across all exports");
    }

    /// Builds an instrument owner backed by `telemetry`, registering all six counters once. When
    /// `telemetry` is the shared [GatewayMetrics#noop] registry the shared [#noop] owner is returned,
    /// so the wire client's `metrics == noop()` guard can short-circuit the retry-accounting context
    /// read when self-observability is disabled.
    ///
    /// @param telemetry the registry the counters report to
    /// @return the instrument owner
    public static ExporterMetrics from(GatewayMetrics telemetry) {
        return telemetry == GatewayMetrics.noop() ? NOOP : new ExporterMetrics(telemetry);
    }

    /// The shared no-op instrument owner, backed by [GatewayMetrics#noop]. Used by a wire client
    /// constructed without a builder (the standalone and lifecycle-test paths), so its retry listener
    /// still has a valid instance to register against.
    ///
    /// @return the shared no-op instance
    public static ExporterMetrics noop() {
        return NOOP;
    }

    /// Records one settled send at the exporter boundary: adds the payload's item count and serialized
    /// size, adds the measured latency, and increments success or failure. A send that failed
    /// exceptionally or returned a [DeliveryResult.Rejected] is a failure; anything else is a success.
    ///
    /// @param payload      the payload that was sent
    /// @param latencyNanos the measured send latency in nanoseconds
    /// @param result       the delivery result, or null when the send failed exceptionally
    /// @param failure      the failure the send completed with, or null on a returned result
    public void recordSend(
            Payload<?> payload,
            long serializedBytes,
            long latencyNanos,
            @Nullable DeliveryResult result,
            @Nullable Throwable failure) {
        items.add(payload.itemCount());
        bytes.add(serializedBytes);
        durationNanos.add(latencyNanos);
        if (failure != null || result instanceof DeliveryResult.Rejected) {
            this.failure.add(1);
        } else {
            success.add(1);
        }
    }

    /// Records one retry attempt beyond the first. Called from the wire client's retry event listener,
    /// once per scheduled retry.
    public void recordRetry() {
        retries.add(1);
    }
}
