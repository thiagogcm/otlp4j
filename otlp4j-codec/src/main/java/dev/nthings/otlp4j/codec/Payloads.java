package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.core.component.PayloadFactory;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// The proto-free codec facade for constructing payloads from domain batches. Its whole public
/// surface names only core types, so a processor module can emit a replacement payload without
/// resolving a proto type or naming a [SignalCodec] variable: the helper picks the right codec
/// internally.
///
/// The receive-path pass-through payload is built directly by the transports through
/// `SignalCodec.<signal>.proto(request)` (transports name proto), so this facade needs no proto
/// factory on its public surface. [#domain] is the sanctioned domain-payload constructor for the
/// built-in and sample processors.
public final class Payloads {

    /// The canonical [PayloadFactory] over [#domain], the emit seam a core domain-transform
    /// processor built by `ProcessorFactory.of` takes so it can wrap a transformed batch without
    /// naming this codec module.
    public static final PayloadFactory DOMAIN = Payloads::domain;

    private Payloads() {}

    /// Builds a domain-carrying payload for `signal` over `batch`, selecting the signal's codec
    /// internally so the caller never names a proto type.
    ///
    /// @param signal the signal that keys the batch
    /// @param batch  the domain batch
    /// @param <T>    the domain batch type
    /// @return a domain-backed telemetry payload
    public static <T> TelemetryPayload domain(SignalDescriptor<T> signal, T batch) {
        return SignalCodec.forDescriptor(signal).domain(batch);
    }
}
