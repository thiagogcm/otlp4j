package dev.nthings.otlp4j.components.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.config.ConfigNode;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BatchProcessorFactoryTest {

    private final BatchProcessorFactory factory = new BatchProcessorFactory();

    @Test
    void metadataIsHonest() {
        assertThat(factory.kind()).isEqualTo(ComponentKind.PROCESSOR);
        assertThat(factory.stability()).isEqualTo(StabilityLevel.BETA);
        assertThat(factory.type()).isEqualTo("batch");
        assertThat(factory.defaultConfig()).isEqualTo(BatchProcessorConfig.defaults());
    }

    @Test
    void supportsEverySignalExceptProfiles() {
        assertThat(factory.supportsSignal(SignalDescriptor.TRACES)).isTrue();
        assertThat(factory.supportsSignal(SignalDescriptor.METRICS)).isTrue();
        assertThat(factory.supportsSignal(SignalDescriptor.LOGS)).isTrue();
        assertThat(factory.supportsSignal(SignalDescriptor.PROFILES)).isFalse();
    }

    @Test
    void bindReadsEveryKeyOntoTheDefaults() {
        var node = ConfigNode.of(Map.ofEntries(
                Map.entry("timeout", "2s"),
                Map.entry("send_batch_size", 4096),
                Map.entry("send_batch_max_size", 5000),
                Map.entry("send_batch_max_bytes", 1_000_000),
                Map.entry("metadata_keys", List.of("tenant", "env")),
                Map.entry("queue_size", 512),
                Map.entry("num_consumers", 3),
                Map.entry("overflow", "drop_oldest"),
                Map.entry("wait_for_result", true),
                Map.entry("degraded_window", "10s")));

        var config = factory.bind(node, factory.defaultConfig());

        assertThat(config.batch().timeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(config.batch().sendBatchSize()).isEqualTo(4096);
        assertThat(config.batch().sendBatchMaxSize()).isEqualTo(5000);
        assertThat(config.batch().sendBatchMaxBytes()).isEqualTo(1_000_000);
        assertThat(config.batch().metadataKeys()).containsExactly("tenant", "env");
        assertThat(config.delivery().queueSize()).isEqualTo(512);
        assertThat(config.delivery().numConsumers()).isEqualTo(3);
        assertThat(config.delivery().overflow()).isEqualTo(OverflowPolicy.DROP_OLDEST);
        assertThat(config.delivery().waitForResult()).isTrue();
        assertThat(config.degradedWindow()).isEqualTo(Duration.ofSeconds(10));
        assertThat(node.unreadKeys()).isEmpty();
    }

    @Test
    void bindFallsBackToDefaultsForAbsentKeys() {
        var config = factory.bind(ConfigNode.empty(), factory.defaultConfig());
        assertThat(config).isEqualTo(factory.defaultConfig());
    }

    @Test
    void bindReportsUnknownKeyAsUnread() {
        var node = ConfigNode.of(Map.of("timeout", "2s", "bogus", "x"));
        factory.bind(node, factory.defaultConfig());
        assertThat(node.unreadKeys()).containsExactly("bogus");
    }

    @Test
    void bindRejectsUnknownOverflowValue() {
        var node = ConfigNode.of(Map.of("overflow", "nonsense"));
        assertThatThrownBy(() -> factory.bind(node, factory.defaultConfig()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
