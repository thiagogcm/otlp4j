package dev.nthings.otlp4j.core.delivery;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import java.util.function.LongSupplier;

/// What the owner of a [QueuedExporter] observes about its queue: losses after admission, refusals
/// returned to the caller, and the live queue depth. The contracts module has no logging or metrics
/// dependency, so the owner decides how each is logged and counted. A refusal is reported separately
/// from a loss because it is a different thing: the sender is told and can retry, whereas a loss is data
/// the exporter had already acknowledged.
public interface QueueObserver extends LossListener {

    /// An observer that ignores everything.
    QueueObserver NONE = (items, reason) -> {};

    /// An enqueue refused by the queue and returned to the caller: the queue was full under `FAIL`, or
    /// had stopped accepting.
    ///
    /// @param items   the items in the refused payload
    /// @param refusal the retryable rejection the caller received
    default void refused(long items, DeliveryResult.Rejected refusal) {}

    /// Registers the live depth of the queue: entries admitted and not yet finished, whether waiting for
    /// a consumer or being delivered or redelivered. Called once when the queue starts; the returned
    /// registration is closed once it has drained.
    ///
    /// @param size the live depth
    /// @return a registration to close when the queue stops
    default GatewayMetrics.Registration depth(LongSupplier size) {
        return () -> {};
    }
}
