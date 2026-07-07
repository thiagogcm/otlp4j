package dev.nthings.otlp4j.components.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.components.internal.MatchSpec;
import org.junit.jupiter.api.Test;

class RoutingConfigTest {

    @Test
    void defaultsForwardEveryPayloadRatherThanSilentlyDroppingEverything() {
        var defaults = RoutingConfig.defaults();
        assertThat(defaults.match().rules()).isEmpty();
        assertThat(defaults.defaultRoute()).isTrue();
    }

    @Test
    void rejectsANullMatch() {
        assertThatThrownBy(() -> new RoutingConfig(null, true)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsAnExplicitMatchWithDefaultRouteFalse() {
        var config = new RoutingConfig(MatchSpec.none(), false);
        assertThat(config.defaultRoute()).isFalse();
    }
}
