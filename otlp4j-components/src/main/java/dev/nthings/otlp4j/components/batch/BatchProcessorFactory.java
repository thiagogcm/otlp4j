package dev.nthings.otlp4j.components.batch;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.SignalSupport;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// The factory for the built-in batch processor, type `batch`.
///
/// It declares its honest signal support through the core [SignalSupport] capability: it supports
/// traces, metrics, and logs, but not profiles, because profiles merge is not lossless across distinct
/// dictionaries. A profiles pipeline that lists `batch` therefore fails graph validation before start
/// rather than losing data at runtime.
public final class BatchProcessorFactory
        implements ProcessorFactory<BatchProcessorConfig>, SignalSupport {

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
    public boolean supportsSignal(SignalDescriptor<?> signal) {
        // Profiles merge throws on distinct non-empty dictionaries, so batching profiles is unsafe.
        return signal != SignalDescriptor.PROFILES;
    }

    @Override
    public <T extends TelemetryPayload> Processor<T> create(
            CreateContext ctx, BatchProcessorConfig config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
        Processor<TelemetryPayload> processor = new BatchProcessor<>(
                ctx.id(),
                ProcessorFactory.asDomainDescriptor(signal),
                config,
                SignalConsumer.asPayload(next),
                System::nanoTime);
        return ProcessorFactory.asTypedProcessor(processor);
    }
}
