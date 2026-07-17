package dev.nthings.otlp4j.core.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
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
        var total = new AtomicLong();
        var gauge = new AtomicReference<LongSupplier>();
        GatewayMetrics registry = new GatewayMetrics() {
            @Override
            public LongCounter counter(String name, String description) {
                return total::addAndGet;
            }

            @Override
            public Registration gauge(String name, String description, LongSupplier value) {
                gauge.set(value);
                return () -> gauge.set(null);
            }
        };
        var live = new AtomicLong(7);

        var counter = registry.counter("otlp4j.test.count", "a count");
        var registration = registry.gauge("otlp4j.test.gauge", "a gauge", live::get);

        counter.add(3);
        counter.add(4);
        assertThat(total.get()).isEqualTo(7);

        live.set(99);
        assertThat(gauge.get().getAsLong()).isEqualTo(99);

        registration.close();
        assertThat(gauge.get()).isNull();
    }
}
