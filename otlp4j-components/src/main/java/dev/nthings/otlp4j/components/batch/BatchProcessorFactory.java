package dev.nthings.otlp4j.components.batch;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.SignalSupport;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.config.ConfigBinder;
import dev.nthings.otlp4j.core.config.ConfigNode;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.List;
import java.util.Locale;

/// The factory for the built-in batch processor, type `batch`, stability `BETA`.
///
/// It declares its honest signal support through the core [SignalSupport] capability: it supports
/// traces, metrics, and logs, but not profiles, because profiles merge is not lossless across distinct
/// dictionaries. A profiles pipeline that lists `batch` therefore fails graph validation before start
/// rather than losing data at runtime. It also implements [ConfigBinder] so a declarative loader binds
/// its Collector-style configuration keys onto the defaults.
public final class BatchProcessorFactory
        implements ProcessorFactory<BatchProcessorConfig>, SignalSupport, ConfigBinder<BatchProcessorConfig> {

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
    public StabilityLevel stability() {
        return StabilityLevel.BETA;
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

    @Override
    public BatchProcessorConfig bind(ConfigNode node, BatchProcessorConfig defaults) {
        var base = defaults.batch();
        var batch = new BatchPolicy(
                node.intValue("send_batch_size", base.sendBatchSize()),
                node.intValue("send_batch_max_size", base.sendBatchMaxSize()),
                node.longValue("send_batch_max_bytes", base.sendBatchMaxBytes()),
                node.duration("timeout", base.timeout()),
                metadataKeys(node, base.metadataKeys()));
        var queue = defaults.delivery();
        var delivery = new DeliveryQueuePolicy(
                node.intValue("queue_size", queue.queueSize()),
                node.intValue("num_consumers", queue.numConsumers()),
                overflow(node, queue.overflow()),
                node.bool("wait_for_result", queue.waitForResult()));
        return new BatchProcessorConfig(batch, delivery, node.duration("degraded_window", defaults.degradedWindow()));
    }

    private static List<String> metadataKeys(ConfigNode node, List<String> defaults) {
        var keys = node.stringList("metadata_keys");
        return keys.isEmpty() ? defaults : keys;
    }

    private static OverflowPolicy overflow(ConfigNode node, OverflowPolicy defaults) {
        return node.string("overflow")
                .map(value -> OverflowPolicy.valueOf(value.trim().toUpperCase(Locale.ROOT)))
                .orElse(defaults);
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
