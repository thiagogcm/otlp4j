package dev.nthings.otlp4j.components.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class MemoryLimiterConfigTest {

    private static final long MIB = 1024L * 1024L;

    @Test
    void defaultsCarryTheDocumentedValues() {
        var config = MemoryLimiterConfig.defaults();
        assertThat(config.checkInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.limitBytes()).isEqualTo(512 * MIB);
        assertThat(config.spikeLimitBytes()).isEqualTo(128 * MIB);
        assertThat(config.retryAfter()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void softThresholdIsLimitMinusSpikeAndHardThresholdIsLimit() {
        var config = new MemoryLimiterConfig(Duration.ofSeconds(1), 512 * MIB, 128 * MIB, Duration.ofSeconds(1));
        assertThat(config.softLimitBytes()).isEqualTo(384 * MIB);
        assertThat(config.hardLimitBytes()).isEqualTo(512 * MIB);
    }

    @Test
    void rejectsANullCheckInterval() {
        assertThatThrownBy(() -> new MemoryLimiterConfig(null, 512 * MIB, 128 * MIB, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checkInterval");
    }

    @Test
    void rejectsAZeroCheckInterval() {
        assertThatThrownBy(() -> new MemoryLimiterConfig(Duration.ZERO, 512 * MIB, 128 * MIB, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checkInterval");
    }

    @Test
    void rejectsANegativeCheckInterval() {
        assertThatThrownBy(() ->
                        new MemoryLimiterConfig(Duration.ofSeconds(-1), 512 * MIB, 128 * MIB, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checkInterval");
    }

    @Test
    void rejectsAZeroLimit() {
        assertThatThrownBy(() -> new MemoryLimiterConfig(Duration.ofSeconds(1), 0, 0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limitBytes");
    }

    @Test
    void rejectsANegativeSpike() {
        assertThatThrownBy(() -> new MemoryLimiterConfig(Duration.ofSeconds(1), 512 * MIB, -1, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("spikeLimitBytes");
    }

    @Test
    void rejectsASpikeNotSmallerThanTheLimit() {
        assertThatThrownBy(() ->
                        new MemoryLimiterConfig(Duration.ofSeconds(1), 512 * MIB, 512 * MIB, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("smaller than limitBytes");
    }

    @Test
    void rejectsANullRetryAfter() {
        assertThatThrownBy(() -> new MemoryLimiterConfig(Duration.ofSeconds(1), 512 * MIB, 128 * MIB, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryAfter");
    }

    @Test
    void rejectsAZeroRetryAfter() {
        assertThatThrownBy(() -> new MemoryLimiterConfig(Duration.ofSeconds(1), 512 * MIB, 128 * MIB, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryAfter");
    }
}
