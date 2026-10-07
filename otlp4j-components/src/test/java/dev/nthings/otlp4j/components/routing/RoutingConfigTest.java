package dev.nthings.otlp4j.components.routing;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RoutingConfigTest {

    @Test
    void rejectsANullMatch() {
        assertThatThrownBy(() -> new RoutingConfig(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
