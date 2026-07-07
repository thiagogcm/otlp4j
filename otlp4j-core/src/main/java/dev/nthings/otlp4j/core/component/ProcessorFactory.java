package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// A factory for processors. It creates one processor bound to a signal, forwarding to the next
/// consumer in the pipeline. The signal and the next consumer are supplied at creation so the same
/// factory can produce a processor for any signal it supports.
///
/// @param <C> the typed configuration this factory accepts
public interface ProcessorFactory<C extends ComponentConfig> extends ComponentFactory<C> {

    /// Creates one processor for `signal` that forwards to `next`.
    ///
    /// @param ctx    the create context (identity and base context)
    /// @param config the processor configuration
    /// @param signal the signal this processor operates on
    /// @param next   the downstream consumer this processor forwards to
    /// @param <T>    the payload type
    /// @return the processor
    <T extends TelemetryPayload> Processor<T> create(
            CreateContext ctx, C config, SignalDescriptor<T> signal, SignalConsumer<T> next);
}
