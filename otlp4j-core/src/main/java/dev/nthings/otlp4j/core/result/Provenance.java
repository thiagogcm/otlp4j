package dev.nthings.otlp4j.core.result;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// Which component produced a delivery result, and for which signal. Every delivery result carries
/// its provenance so a diagnostic can name the failing component, and so fan-out can preserve the
/// originating component of a propagated rejection.
///
/// @param componentId the producing component's identity
/// @param kind        the producing component's kind
/// @param signal      the signal the result is for
public record Provenance(ComponentId componentId, ComponentKind kind, SignalDescriptor<?> signal) {

    /// Validates that every component is present.
    public Provenance {
        if (componentId == null || kind == null || signal == null) {
            throw new IllegalArgumentException("provenance components must be non-null");
        }
    }
}
