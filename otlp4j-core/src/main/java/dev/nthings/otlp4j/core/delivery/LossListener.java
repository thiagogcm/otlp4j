package dev.nthings.otlp4j.core.delivery;

/// Told about telemetry a [DeliveryQueue] acknowledged and then lost. The queue sits in the contracts
/// module, which has no logging or metrics dependency, so the owning component supplies the listener
/// and decides how a loss is logged and counted.
///
/// A listener runs on a delivery worker or an admitting caller, so it must not block, and a failure it
/// throws is ignored: a broken instrument must not stop the queue from delivering the entries behind
/// the lost one.
@FunctionalInterface
public interface LossListener {

    /// A loss after admission: a failed or partially rejected delivery, an entry evicted by
    /// `DROP_OLDEST`, one discarded by `DROP_NEWEST`, one given up when redelivery ran out of time or
    /// was cut short by shutdown, or one abandoned by a fatal worker error. A refusal returned to the
    /// caller is not reported here, because the caller owns it.
    ///
    /// @param items  the number of telemetry items lost
    /// @param reason a human-readable cause
    void lost(long items, String reason);
}
