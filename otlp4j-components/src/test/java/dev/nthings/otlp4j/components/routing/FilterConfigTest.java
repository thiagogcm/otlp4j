package dev.nthings.otlp4j.components.routing;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class FilterConfigTest {

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
