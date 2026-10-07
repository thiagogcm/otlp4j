package dev.nthings.otlp4j.core.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Reject unusable policy inputs and isolate mutable batching metadata.
class PolicyTest {

    @Test
    @DisplayName("backoff rejects nonpositive delays, shrinking multipliers, and invalid jitter")
    void backoffPolicy() {
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
    @DisplayName("retry rejects a missing backoff or a zero attempt budget")
    void retryPolicy() {
        assertThatThrownBy(() -> new RetryPolicy(0, BackoffPolicy.defaults(), true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, null, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("timeout policy requires a positive per-attempt and a total at least as large")
    void timeoutPolicy() {

        assertThatThrownBy(() -> new TimeoutPolicy(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TimeoutPolicy(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("batch policy validates its sizes and defensively copies the metadata keys")
    void batchPolicy() {
        var keys = new ArrayList<>(List.of("x-tenant"));
        var batch = new BatchPolicy(100, 200, 1_000_000, Duration.ofSeconds(1), keys);
        keys.add("late");
        assertThat(batch.metadataKeys()).containsExactly("x-tenant");

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

        assertThatThrownBy(() -> new DeliveryQueuePolicy(0, 1, OverflowPolicy.FAIL))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeliveryQueuePolicy(1, 0, OverflowPolicy.FAIL))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeliveryQueuePolicy(1, 1, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
