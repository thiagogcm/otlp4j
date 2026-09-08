package dev.nthings.otlp4j.components.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RoutingConfigTest {

    @Test
    void defaultsMatchNothingUntilConfigured() {
        assertThat(RoutingConfig.defaults().match().rules()).isEmpty();
    }

    @Test
    void rejectsANullMatch() {
        assertThatThrownBy(() -> new RoutingConfig(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void catchAllIsExplicit() {
        var config = new RoutingConfig(MatchSpec.of(MatchRule.matchAll()));
        assertThat(config.match().rules()).containsExactly(MatchRule.matchAll());
    }
}
