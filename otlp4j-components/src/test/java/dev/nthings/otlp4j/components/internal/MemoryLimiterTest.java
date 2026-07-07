package dev.nthings.otlp4j.components.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.components.internal.MemoryLimiter.State;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MemoryLimiterTest {

    private final AtomicLong used = new AtomicLong();
    private final MemoryLimiter limiter = new MemoryLimiter(used::get, 100, 200);

    @Test
    void startsNormalBeforeAnySample() {
        assertThat(limiter.state()).isEqualTo(State.NORMAL);
    }

    @Test
    void samplesNormalBelowTheSoftThreshold() {
        used.set(99);
        assertThat(limiter.refresh()).isEqualTo(State.NORMAL);
        assertThat(limiter.state()).isEqualTo(State.NORMAL);
    }

    @Test
    void samplesSoftAtTheSoftThreshold() {
        used.set(100);
        assertThat(limiter.refresh()).isEqualTo(State.SOFT);
    }

    @Test
    void samplesSoftBetweenTheThresholds() {
        used.set(150);
        assertThat(limiter.refresh()).isEqualTo(State.SOFT);
    }

    @Test
    void samplesHardAtTheHardThreshold() {
        used.set(200);
        assertThat(limiter.refresh()).isEqualTo(State.HARD);
    }

    @Test
    void recoversToNormalWhenUsedMemoryDropsBackUnderSoft() {
        used.set(250);
        assertThat(limiter.refresh()).isEqualTo(State.HARD);
        used.set(50);
        assertThat(limiter.refresh()).isEqualTo(State.NORMAL);
    }

    @Test
    void exposesItsThresholds() {
        assertThat(limiter.softLimitBytes()).isEqualTo(100);
        assertThat(limiter.hardLimitBytes()).isEqualTo(200);
    }

    @Test
    void heapGaugeReportsAPositiveUsedFigure() {
        var used = MemoryLimiter.heapGauge().getAsLong();
        assertThat(used).isPositive();
    }

    @Test
    void rejectsANullGauge() {
        assertThatThrownBy(() -> new MemoryLimiter(null, 100, 200))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("usedBytes");
    }

    @Test
    void rejectsASoftThresholdAboveTheHardThreshold() {
        assertThatThrownBy(() -> new MemoryLimiter(used::get, 300, 200))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("softLimitBytes");
    }
}
