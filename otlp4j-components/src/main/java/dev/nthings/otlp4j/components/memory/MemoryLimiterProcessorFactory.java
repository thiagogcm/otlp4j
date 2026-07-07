package dev.nthings.otlp4j.components.memory;

import dev.nthings.otlp4j.components.internal.MemoryLimiter;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.config.ConfigBinder;
import dev.nthings.otlp4j.core.config.ConfigNode;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;

/// The factory for the built-in memory limiter processor, type `memory_limiter`, stability `BETA`.
///
/// It supports every signal, because the limiter inspects nothing in the payload; it only reacts to
/// sampled memory pressure. It implements [ConfigBinder] so a declarative loader binds its
/// Collector-style configuration keys (`check_interval`, `limit_mib`, `spike_limit_mib`, `retry_after`)
/// onto the defaults, converting the mebibyte-denominated limits to bytes.
public final class MemoryLimiterProcessorFactory
        implements ProcessorFactory<MemoryLimiterConfig>, ConfigBinder<MemoryLimiterConfig> {

    /// The component type name.
    public static final String TYPE = "memory_limiter";

    private static final long MIB = 1024L * 1024L;

    /// Creates a memory limiter processor factory.
    public MemoryLimiterProcessorFactory() {
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
                payloadConsumer(next),
                System::nanoTime,
                System::gc);
        return castProcessor(processor);
    }

    @Override
    public MemoryLimiterConfig bind(ConfigNode node, MemoryLimiterConfig defaults) {
        return new MemoryLimiterConfig(
                node.duration("check_interval", defaults.checkInterval()),
                mebibytes(node, "limit_mib", defaults.limitBytes()),
                mebibytes(node, "spike_limit_mib", defaults.spikeLimitBytes()),
                node.duration("retry_after", defaults.retryAfter()));
    }

    /// Reads a mebibyte-denominated integer key and converts it to bytes, falling back to the default
    /// byte value (rendered as whole mebibytes) when the key is absent.
    private static long mebibytes(ConfigNode node, String key, long defaultBytes) {
        return (long) node.intValue(key, Math.toIntExact(defaultBytes / MIB)) * MIB;
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
