package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.delivery.QueueObserver;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The exporter's side of a [dev.nthings.otlp4j.core.delivery.QueuedExporter]: the queue lives in the
/// contracts module and cannot log or count, so this logs each loss at WARN with the component id, item
/// count and reason, and writes the loss, refusal and depth instruments in [ExporterMetrics].
public final class QueueEvents implements QueueObserver {

    private static final Logger LOG = LoggerFactory.getLogger(QueueEvents.class);

    private final ComponentId id;
    private final SignalDescriptor<?> signal;
    private final ExporterMetrics metrics;

    /// Observes the queue of exporter `id` for `signal`.
    ///
    /// @param id      the exporter's component id
    /// @param signal  the signal the queue carries
    /// @param metrics the instrument owner shared by all of the exporter's queues
    public QueueEvents(ComponentId id, SignalDescriptor<?> signal, ExporterMetrics metrics) {
        this.id = id;
        this.signal = signal;
        this.metrics = metrics;
    }

    @Override
    public void lost(long items, String reason) {
        LOG.warn("exporter {} ({}) lost {} items after acknowledging them: {}", id, signal, items, reason);
        metrics.recordQueueLost(items);
    }

    @Override
    public void refused(long items, DeliveryResult.Rejected refusal) {
        metrics.recordQueueRefused(items);
    }

    @Override
    public GatewayMetrics.Registration depth(LongSupplier size) {
        return metrics.registerQueueSize(size);
    }
}
