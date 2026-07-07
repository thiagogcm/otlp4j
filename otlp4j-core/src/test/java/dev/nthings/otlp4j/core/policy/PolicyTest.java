package dev.nthings.otlp4j.core.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the neutral policy records and enums: their defaults, their invariants, and the
/// preserved legacy default numbers.
class PolicyTest {

    @Test
    @DisplayName("backoff defaults mirror the legacy client retry numbers and enforce ranges")
    void backoffPolicy() {
        var defaults = BackoffPolicy.defaults();
        assertThat(defaults.initial()).isEqualTo(Duration.ofSeconds(1));
        assertThat(defaults.multiplier()).isEqualTo(1.5);
        assertThat(defaults.jitter()).isEqualTo(0.5);
        assertThat(defaults.max()).isEqualTo(Duration.ofSeconds(5));

        assertThatThrownBy(() -> new BackoffPolicy(Duration.ZERO, 1.5, 0.5, Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(2), 1.5, 0.5, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(1), 0.5, 0.5, Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(1), 1.5, 1.5, Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("retry defaults are five attempts honouring the server hint and enforce a minimum")
    void retryPolicy() {
        var defaults = RetryPolicy.defaults();
        assertThat(defaults.maxAttempts()).isEqualTo(5);
        assertThat(defaults.respectServerRetryAfter()).isTrue();
        assertThat(defaults.backoff()).isEqualTo(BackoffPolicy.defaults());

        assertThatThrownBy(() -> new RetryPolicy(0, BackoffPolicy.defaults(), true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, null, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("timeout policy requires a positive per-attempt and a total at least as large")
    void timeoutPolicy() {
        var timeout = new TimeoutPolicy(Duration.ofSeconds(5), Duration.ofSeconds(30));
        assertThat(timeout.perAttempt()).isEqualTo(Duration.ofSeconds(5));
        assertThat(timeout.total()).isEqualTo(Duration.ofSeconds(30));

        assertThatThrownBy(() -> new TimeoutPolicy(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TimeoutPolicy(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("batch policy validates its sizes and defensively copies the metadata keys")
    void batchPolicy() {
        var keys = new java.util.ArrayList<>(List.of("x-tenant"));
        var batch = new BatchPolicy(100, 200, 1_000_000, Duration.ofSeconds(1), keys);
        keys.add("late");
        assertThat(batch.metadataKeys()).containsExactly("x-tenant");
        assertThat(batch.sendBatchSize()).isEqualTo(100);
        assertThat(batch.sendBatchMaxSize()).isEqualTo(200);
        assertThat(batch.sendBatchMaxBytes()).isEqualTo(1_000_000);

        // A zero cap means no cap and is allowed.
        assertThat(new BatchPolicy(50, 0, 0, Duration.ZERO, List.of()).sendBatchMaxSize()).isZero();

        assertThatThrownBy(() -> new BatchPolicy(0, 0, 0, Duration.ZERO, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BatchPolicy(100, 50, 0, Duration.ZERO, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BatchPolicy(1, -1, 0, Duration.ZERO, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BatchPolicy(1, 0, -1, Duration.ZERO, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BatchPolicy(1, 0, 0, null, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BatchPolicy(1, 0, 0, Duration.ZERO, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("delivery queue policy validates its sizes and overflow")
    void deliveryQueuePolicy() {
        var queue = new DeliveryQueuePolicy(1000, 4, OverflowPolicy.BLOCK, true);
        assertThat(queue.queueSize()).isEqualTo(1000);
        assertThat(queue.numConsumers()).isEqualTo(4);
        assertThat(queue.overflow()).isEqualTo(OverflowPolicy.BLOCK);
        assertThat(queue.waitForResult()).isTrue();

        assertThatThrownBy(() -> new DeliveryQueuePolicy(0, 1, OverflowPolicy.FAIL, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeliveryQueuePolicy(1, 0, OverflowPolicy.FAIL, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeliveryQueuePolicy(1, 1, null, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("the overflow enum has the four relocated values")
    void overflowPolicy() {
        assertThat(OverflowPolicy.values())
                .containsExactly(
                        OverflowPolicy.DROP_OLDEST,
                        OverflowPolicy.DROP_NEWEST,
                        OverflowPolicy.BLOCK,
                        OverflowPolicy.FAIL);
        for (var value : OverflowPolicy.values()) {
            assertThat(OverflowPolicy.valueOf(value.name())).isEqualTo(value);
        }
    }
}
