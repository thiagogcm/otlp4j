package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("TapOptions")
class TapOptionsTest {

    @DisplayName("a null strategy and a non-positive buffer size are rejected")
    @Test
    void invalidArgumentsRejected() {
        assertThatThrownBy(() -> new TapOptions(null, 16))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new TapOptions(OverflowPolicy.DROP_OLDEST, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bufferSize must be >= 1");
    }
}
