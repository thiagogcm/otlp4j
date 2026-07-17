package dev.nthings.otlp4j.transport.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("TapOptions")
class TapOptionsTest {

    @DisplayName("defaults() is DROP_OLDEST with a 256-element buffer")
    @Test
    void defaultsAreDropOldest256() {
        var defaults = TapOptions.defaults();
        assertThat(defaults.strategy()).isEqualTo(OverflowPolicy.DROP_OLDEST);
        assertThat(defaults.bufferSize()).isEqualTo(256);
    }

    @DisplayName("the three non-blocking overflow policies are accepted")
    @Test
    void nonBlockingPoliciesAccepted() {
        assertThat(new TapOptions(OverflowPolicy.DROP_OLDEST, 1).strategy()).isEqualTo(OverflowPolicy.DROP_OLDEST);
        assertThat(new TapOptions(OverflowPolicy.DROP_NEWEST, 4).strategy()).isEqualTo(OverflowPolicy.DROP_NEWEST);
        assertThat(new TapOptions(OverflowPolicy.FAIL, 8).strategy()).isEqualTo(OverflowPolicy.FAIL);
    }

    @DisplayName("BLOCK is rejected because the tap sits outside the ack path")
    @Test
    void blockRejectedByTap() {
        assertThatThrownBy(() -> new TapOptions(OverflowPolicy.BLOCK, 16))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BLOCK is unavailable");
    }

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
