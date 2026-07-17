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
                ctx.id(), domainDescriptor(signal), config, payloadConsumer(next), System::nanoTime);
        return castProcessor(processor);
    }

    /// Reinterprets the payload-typed descriptor the graph supplies as the domain-typed descriptor the
    /// processor operates on. At runtime the descriptor is the signal's domain-batch descriptor, so
    /// this reinterpretation is safe, mirroring how the sample function processor factory is wired.
    @SuppressWarnings("unchecked")
    private static <M> SignalDescriptor<M> domainDescriptor(SignalDescriptor<?> signal) {
        return (SignalDescriptor<M>) signal;
    }

    @SuppressWarnings("unchecked")
    private static SignalConsumer<TelemetryPayload> payloadConsumer(SignalConsumer<?> next) {
        return (SignalConsumer<TelemetryPayload>) next;
    }

    @SuppressWarnings("unchecked")
    private static <T extends TelemetryPayload> Processor<T> castProcessor(Processor<TelemetryPayload> processor) {
        return (Processor<T>) (Processor<?>) processor;
    }
}
