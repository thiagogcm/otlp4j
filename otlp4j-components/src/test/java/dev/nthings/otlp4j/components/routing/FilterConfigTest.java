package dev.nthings.otlp4j.components.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.components.internal.MatchSpec;
import org.junit.jupiter.api.Test;

class FilterConfigTest {

    @Test
    void defaultsAreATransparentPassThrough() {
        var defaults = FilterConfig.defaults();
        assertThat(defaults.include().rules()).isEmpty();
        assertThat(defaults.exclude().rules()).isEmpty();
    }

    @Test
    void rejectsANullInclude() {
        assertThatThrownBy(() -> new FilterConfig(null, MatchSpec.none()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsANullExclude() {
        assertThatThrownBy(() -> new FilterConfig(MatchSpec.none(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
