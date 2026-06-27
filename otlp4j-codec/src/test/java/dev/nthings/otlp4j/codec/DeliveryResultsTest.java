package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.ConsumeResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("DeliveryResults")
class DeliveryResultsTest {

    @DisplayName("retryable rejection has no cause")
    @Test
    void retryableRejection() {
        var rejected = (ConsumeResult.Rejected<?>) (ConsumeResult<?>) ConsumeResult.retryableRejected("backpressure");
        assertThat(DeliveryResults.isRetryable(rejected)).isTrue();
        assertThat(DeliveryResults.httpStatus(rejected)).isEqualTo(503);
    }

    @DisplayName("permanent rejection carries a cause")
    @Test
    void permanentRejection() {
        var rejected = (ConsumeResult.Rejected<?>) (ConsumeResult<?>) ConsumeResult
                .permanentRejected("validation failed", new IllegalStateException());
        assertThat(DeliveryResults.isRetryable(rejected)).isFalse();
        assertThat(DeliveryResults.httpStatus(rejected)).isEqualTo(500);
    }
}
