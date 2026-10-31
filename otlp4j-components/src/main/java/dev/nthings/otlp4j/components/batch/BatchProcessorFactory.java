package dev.nthings.otlp4j.components.batch;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// The factory for the built-in batch processor, type `batch`, which supports every signal.
public final class BatchProcessorFactory implements ProcessorFactory<BatchProcessorConfig> {

    /// The component type name.
    public static final String TYPE = "batch";

    /// Creates a batch processor factory.
    public BatchProcessorFactory() {
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
    public BatchProcessorConfig defaultConfig() {
        return BatchProcessorConfig.defaults();
    }

    @Override
    public <T> Processor<T> create(
            CreateContext ctx, BatchProcessorConfig config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
        return new BatchProcessor<>(ctx.id(), signal, config, next, System::nanoTime);
    }
}
