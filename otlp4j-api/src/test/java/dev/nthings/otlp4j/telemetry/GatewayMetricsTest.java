package dev.nthings.otlp4j.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("GatewayMetrics")
class GatewayMetricsTest {

    @DisplayName("noop() is a shared singleton whose instruments do nothing")
    @Test
    void noopIsSharedAndInert() {
        var noop = GatewayMetrics.noop();
        assertThat(GatewayMetrics.noop()).isSameAs(noop);

        var counter = noop.counter("otlp4j.test.count", "a count");
        var registration = noop.gauge("otlp4j.test.gauge", "a gauge", () -> 42L);

        // The no-op instruments must never throw and must hold no state.
        assertThatCode(() -> counter.add(5)).doesNotThrowAnyException();
        assertThatCode(registration::close).doesNotThrowAnyException();
        assertThatCode(registration::close).doesNotThrowAnyException();
    }

    @DisplayName("a real registry accumulates counter deltas and samples the live gauge value")
    @Test
    void realRegistryCountsAndSamples() {
        var registry = new LoggingGatewayMetrics();
        var live = new AtomicLong(7);

        var counter = registry.counter("otlp4j.test.count", "a count");
        var registration = registry.gauge("otlp4j.test.gauge", "a gauge", live::get);

        counter.add(3);
        counter.add(4);
        assertThat(registry.counterValue("otlp4j.test.count")).isEqualTo(7);

        live.set(99);
        assertThat(registry.gaugeValue("otlp4j.test.gauge")).isEqualTo(99);

        registration.close();
        assertThat(registry.gaugeValue("otlp4j.test.gauge")).isZero();
    }
}
