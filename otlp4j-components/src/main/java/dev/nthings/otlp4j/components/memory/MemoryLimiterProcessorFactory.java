package dev.nthings.otlp4j.components.memory;

import dev.nthings.otlp4j.components.internal.MemoryLimiter;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// The factory for the built-in memory limiter processor, type `memory_limiter`.
///
/// It supports every signal, because the limiter inspects nothing in the payload; it only reacts to
/// sampled memory pressure.
public final class MemoryLimiterProcessorFactory implements ProcessorFactory<MemoryLimiterConfig> {

    /// The component type name.
    public static final String TYPE = "memory_limiter";

    /// Creates a memory limiter processor factory.
    public MemoryLimiterProcessorFactory() {
    }

    @Override
    public ComponentKind kind() {
        return ComponentKind.PROCESSOR;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public MemoryLimiterConfig defaultConfig() {
        return MemoryLimiterConfig.defaults();
    }

    @Override
    public <T extends TelemetryPayload> Processor<T> create(
            CreateContext ctx, MemoryLimiterConfig config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
        var limiter = new MemoryLimiter(
                MemoryLimiter.heapGauge(), config.softLimitBytes(), config.hardLimitBytes());
        Processor<TelemetryPayload> processor = new MemoryLimiterProcessor(
                ctx.id(),
                signal,
                limiter,
                config.checkInterval(),
                config.retryAfter(),
                ProcessorFactory.asPayloadConsumer(next),
                System::nanoTime,
                System::gc);
        return ProcessorFactory.asTypedProcessor(processor);
    }
}
