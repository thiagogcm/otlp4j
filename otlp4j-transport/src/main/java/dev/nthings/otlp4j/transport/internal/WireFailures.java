package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// Turns a failure that escaped the dispatch seam into the [DeliveryResult.Rejected] the wire mapping
/// already knows how to render, so an exceptional path and a returned rejection share one mapping.
///
/// The classification is `DeliveryResult.fromFailure`: a failure nobody classified is retryable
/// (`UNAVAILABLE` / `503`), as in the OpenTelemetry Collector's receiver, so an upstream sender retries
/// rather than dropping data over a fault that may be transient.
///
/// **Internal.** Part of the transport layer.
final class WireFailures {

    /// The wire layer does not know which component it serves. The provenance of a result that is
    /// rendered straight onto the wire is never surfaced, so the default receiver id stands in.
    private static final ComponentId WIRE_BOUNDARY = ComponentId.parse("otlp");

    private WireFailures() {}

    /// Classifies `failure` as a rejection for `signal`. A fatal [Error] is not rethrown: it is a
    /// permanent rejection, because the caller is a response callback and must still answer the client.
    ///
    /// @param failure the failure that escaped the dispatcher
    /// @param signal  the signal of the request being served
    /// @return the rejection to render onto the wire
    static DeliveryResult.Rejected rejection(Throwable failure, SignalDescriptor<?> signal) {
        var provenance = new Provenance(WIRE_BOUNDARY, ComponentKind.RECEIVER, signal);
        try {
            return (DeliveryResult.Rejected) DeliveryResult.fromFailure("dispatch failed", failure, provenance);
        } catch (Error fatal) {
            return (DeliveryResult.Rejected) DeliveryResult.permanent("dispatch failed: " + fatal, fatal, provenance);
        }
    }
}
