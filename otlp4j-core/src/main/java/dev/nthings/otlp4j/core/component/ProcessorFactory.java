package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// A factory for processors. It creates one typed processor bound to a signal and forwarding to the
/// next consumer in that signal's pipeline.
///
/// @param <C> the typed configuration this factory accepts
public interface ProcessorFactory<C extends ComponentConfig> extends ComponentFactory<C> {

    /// Creates one processor for `signal` that forwards to `next`.
    ///
    /// @param ctx    the create context
    /// @param config the processor configuration
    /// @param signal the signal this processor operates on
    /// @param next   the downstream consumer
    /// @param <T>    the signal's domain batch type
    /// @return the processor
    <T> Processor<T> create(
            CreateContext ctx, C config, SignalDescriptor<T> signal, SignalConsumer<T> next);

    /// Builds a typed mapping processor. The mapper must return a non-null replacement batch.
    static <M> ProcessorFactory<ComponentConfig.None> map(
            String type, SignalDescriptor<M> signal, UnaryOperator<M> mapper) {
        return new FunctionProcessorFactory<>(type, signal, mapper, null);
    }

    /// Builds a typed filtering processor. A false predicate drops the payload as an accepted local
    /// outcome and does not invoke the downstream consumer.
    static <M> ProcessorFactory<ComponentConfig.None> filter(
            String type, SignalDescriptor<M> signal, Predicate<M> predicate) {
        return new FunctionProcessorFactory<>(type, signal, null, predicate);
    }
}
