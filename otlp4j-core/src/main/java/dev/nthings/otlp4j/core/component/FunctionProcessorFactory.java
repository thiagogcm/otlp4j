package dev.nthings.otlp4j.core.component;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// Package-private implementation behind [ProcessorFactory#map] and [ProcessorFactory#filter].
final class FunctionProcessorFactory<M> implements ProcessorFactory<ComponentConfig.None>, SignalSupport {

    private final String type;
    private final SignalDescriptor<M> signal;
    private final UnaryOperator<M> mapper;
    private final Predicate<M> predicate;

    FunctionProcessorFactory(
            String type, SignalDescriptor<M> signal, UnaryOperator<M> mapper, Predicate<M> predicate) {
        if (type == null || signal == null || (mapper == null) == (predicate == null)) {
            throw new IllegalArgumentException("type, signal, and exactly one operation must be non-null");
        }
        this.type = type;
        this.signal = signal;
        this.mapper = mapper;
        this.predicate = predicate;
    }

    @Override
    public ComponentKind kind() {
        return ComponentKind.PROCESSOR;
    }

    @Override
    public ComponentConfig.None defaultConfig() {
        return ComponentConfig.NONE;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public boolean supportsSignal(SignalDescriptor<?> requestedSignal) {
        return signal == requestedSignal;
    }

    @Override
    public <T> Processor<T> create(
            CreateContext ctx, ComponentConfig.None config,
            SignalDescriptor<T> requestedSignal, SignalConsumer<T> next) {
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(requestedSignal, "requestedSignal");
        Objects.requireNonNull(next, "next");
        if (!supportsSignal(requestedSignal)) {
            throw new IllegalArgumentException(
                    "processor " + type + " operates on " + signal + " but was created for " + requestedSignal);
        }
        @SuppressWarnings("unchecked")
        var typedMapper = (UnaryOperator<T>) mapper;
        @SuppressWarnings("unchecked")
        var typedPredicate = (Predicate<T>) predicate;
        return new FunctionProcessor<>(ctx.id(), requestedSignal, typedMapper, typedPredicate, next);
    }
}
