package dev.nthings.otlp4j.components.memory;

import static dev.nthings.otlp4j.components.support.BatchSupport.baseContext;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.config.ConfigNode;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MemoryLimiterProcessorFactoryTest {

    private static final long MIB = 1024L * 1024L;

    private final MemoryLimiterProcessorFactory factory = new MemoryLimiterProcessorFactory();

    @Test
    void metadataIsHonest() {
        assertThat(factory.kind()).isEqualTo(ComponentKind.PROCESSOR);
        assertThat(factory.stability()).isEqualTo(StabilityLevel.BETA);
        assertThat(factory.type()).isEqualTo("memory_limiter");
        assertThat(factory.defaultConfig()).isEqualTo(MemoryLimiterConfig.defaults());
        assertThat(factory.defaultConfig()).isEqualTo(factory.defaultConfig());
    }

    @Test
    void createsAProcessorForEverySignal() {
        assertThat(create(SignalDescriptor.TRACES)).isNotNull();
        assertThat(create(SignalDescriptor.METRICS)).isNotNull();
        assertThat(create(SignalDescriptor.LOGS)).isNotNull();
        assertThat(create(SignalDescriptor.PROFILES)).isNotNull();
    }

    @Test
    void bindReadsEveryKeyOntoTheDefaults() {
        var node = ConfigNode.of(Map.of(
                "check_interval", "2s",
                "limit_mib", 256,
                "spike_limit_mib", 64,
                "retry_after", "3s"));

        var config = factory.bind(node, factory.defaultConfig());

        assertThat(config.checkInterval()).isEqualTo(Duration.ofSeconds(2));
        assertThat(config.limitBytes()).isEqualTo(256 * MIB);
        assertThat(config.spikeLimitBytes()).isEqualTo(64 * MIB);
        assertThat(config.retryAfter()).isEqualTo(Duration.ofSeconds(3));
        assertThat(node.unreadKeys()).isEmpty();
    }

    @Test
    void bindFallsBackToDefaultsForAbsentKeys() {
        var config = factory.bind(ConfigNode.empty(), factory.defaultConfig());
        assertThat(config).isEqualTo(factory.defaultConfig());
    }

    @Test
    void bindReportsUnknownKeyAsUnread() {
        var node = ConfigNode.of(Map.of("limit_mib", 256, "bogus", "x"));
        factory.bind(node, factory.defaultConfig());
        assertThat(node.unreadKeys()).containsExactly("bogus");
    }

    @SuppressWarnings("unchecked")
    private Processor<TelemetryPayload> create(SignalDescriptor<?> signal) {
        var ctx = new CreateContext(ComponentId.parse("memory_limiter"), baseContext());
        var typed = (SignalDescriptor<TelemetryPayload>) signal;
        SignalConsumer<TelemetryPayload> next = new RecordingConsumer();
        return factory.create(ctx, factory.defaultConfig(), typed, next);
    }
}
