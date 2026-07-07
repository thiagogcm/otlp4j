package dev.nthings.otlp4j.service.config;

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
public interface SignalSupport {

    /// Whether the component this factory produces supports `signal`.
    ///
    /// @param signal the signal to check
    /// @return true if the signal is supported
    boolean supportsSignal(SignalDescriptor<?> signal);
}
