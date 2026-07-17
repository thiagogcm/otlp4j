package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// The seam a domain-transform component uses to wrap a replacement domain batch as a
/// [TelemetryPayload] without naming the codec. Building a payload is a codec concern (only the codec
/// can materialise the proto export request a payload serialises), and `otlp4j-core` must not depend
/// on `otlp4j-codec`, so a core factory that emits a transformed batch takes one of these instead of
/// constructing the payload itself. The codec supplies the canonical implementation (`Payloads.DOMAIN`)
/// and every caller that already resolves the codec passes it to [ProcessorFactory#of].
@FunctionalInterface
public interface PayloadFactory {

    /// Wraps `batch` as a domain-backed payload for `signal`.
    ///
    /// @param signal the signal that keys the batch
    /// @param batch  the domain batch
    /// @param <T>    the domain batch type
    /// @return a domain-backed telemetry payload carrying `batch`
    <T> TelemetryPayload domain(SignalDescriptor<T> signal, T batch);
}
