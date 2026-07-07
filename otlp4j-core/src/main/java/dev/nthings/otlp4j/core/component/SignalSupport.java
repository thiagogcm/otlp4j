package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// An optional capability a receiver, processor, or exporter factory may implement to declare which
/// signals it supports, so the graph can validate signal support before any component is created.
///
/// The core factory contracts for receivers, processors, and exporters declare no per-signal support
/// method: their `create` methods are generic over the payload type, which reads as supporting every
/// signal. A connector, by contrast, declares its `supportedPairs` in core. This interface fills that
/// gap for the other three kinds without widening the core contracts: a factory that supports only a
/// subset of the four signals implements it, and the validator consults it. A factory that does not
/// implement it is treated as supporting every signal, which matches the generic `create` signature.
///
/// This capability lives in core so a built-in factory in a module that does not require the service
/// graph can declare it, while the service-side registry still consults it through an `instanceof`
/// check. Relocating it here from the service module is what lets the batch processor factory declare
/// its honest profiles exclusion from the component module.
public interface SignalSupport {

    /// Whether the component this factory produces supports `signal`.
    ///
    /// @param signal the signal to check
    /// @return true if the signal is supported
    boolean supportsSignal(SignalDescriptor<?> signal);
}
