package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongSupplier;

/// One `otlp4j.exporter.queue.size` gauge per [GatewayMetrics] registry, summing every exporter queue
/// that reports to it. A [GatewayMetrics] gauge is keyed by name alone, so registering one per queue
/// would let each registration replace the last, and an exporter has a queue per signal. Instead the
/// first queue registers the gauge and the last one to stop unregisters it, and in between the gauge
/// reads the total depth across all of them. Counters need no such care, since identically named
/// counters already accumulate into one instrument.
final class QueueDepthGauge {

    private static final Map<GatewayMetrics, QueueDepthGauge> BY_REGISTRY = new IdentityHashMap<>();

    private final GatewayMetrics telemetry;
    private final List<LongSupplier> sizes = new CopyOnWriteArrayList<>();
    private GatewayMetrics.Registration registration;

    private QueueDepthGauge(GatewayMetrics telemetry) {
        this.telemetry = telemetry;
    }

    /// Adds `size` to the registry's depth gauge, registering the gauge for the first queue.
    ///
    /// @param telemetry the registry to report to
    /// @param size      the live depth of one queue
    /// @return a registration that removes this queue and, for the last one, the gauge itself
    static GatewayMetrics.Registration register(GatewayMetrics telemetry, LongSupplier size) {
        synchronized (BY_REGISTRY) {
            var gauge = BY_REGISTRY.computeIfAbsent(telemetry, QueueDepthGauge::new);
            gauge.sizes.add(size);
            if (gauge.registration == null) {
                gauge.registration = telemetry.gauge(
                        "otlp4j.exporter.queue.size",
                        "telemetry entries accepted by an exporter queue and not yet delivered or given up",
                        gauge::total);
            }
            return () -> gauge.remove(size);
        }
    }

    private long total() {
        var total = 0L;
        for (var size : sizes) {
            total += size.getAsLong();
        }
        return total;
    }

    private void remove(LongSupplier size) {
        synchronized (BY_REGISTRY) {
            if (sizes.remove(size) && sizes.isEmpty()) {
                registration.close();
                registration = null;
                BY_REGISTRY.remove(telemetry, this);
            }
        }
    }
}
