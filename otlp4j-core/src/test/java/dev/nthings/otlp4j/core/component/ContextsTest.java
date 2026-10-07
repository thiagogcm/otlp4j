package dev.nthings.otlp4j.core.component;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the lifecycle and creation contexts and their validation.
class ContextsTest {

    private static final Clock CLOCK = Clock.systemUTC();

    @Test
    @DisplayName("a component context carries the metrics handle and clock and rejects nulls")
    void componentContext() {
        var metrics = GatewayMetrics.noop();

        assertThatThrownBy(() -> new ComponentContext(null, CLOCK))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ComponentContext(metrics, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("flush and shutdown contexts carry a non-negative deadline")
    void flushAndShutdownContexts() {

        assertThatThrownBy(() -> new FlushContext(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlushContext(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShutdownContext(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShutdownContext(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a create context carries the id and base context and rejects nulls")
    void createContext() {
        var id = new ComponentId("otlp", "");
        var base = new ComponentContext(GatewayMetrics.noop(), Clock.tickSeconds(ZoneOffset.UTC));

        assertThatThrownBy(() -> new CreateContext(null, base)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CreateContext(id, null)).isInstanceOf(IllegalArgumentException.class);
    }
}
