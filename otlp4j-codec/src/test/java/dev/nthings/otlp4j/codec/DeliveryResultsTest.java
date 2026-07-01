package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.ConsumeResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("DeliveryResults")
class DeliveryResultsTest {

    @DisplayName("retryable rejection maps to 503")
    @Test
    void retryableRejection() {
        var rejected = (ConsumeResult.Rejected<?>) (ConsumeResult<?>) ConsumeResult.retryable("backpressure");
        assertThat(DeliveryResults.isRetryable(rejected)).isTrue();
        assertThat(DeliveryResults.httpStatus(rejected)).isEqualTo(503);
    }

    @DisplayName("a retryable rejection carrying a cause still maps to 503, not 500")
    @Test
    void retryableWithCauseStays503() {
        var rejected = (ConsumeResult.Rejected<?>) (ConsumeResult<?>) ConsumeResult
                .retryable("downstream briefly down", new java.io.IOException("connect timed out"));
        assertThat(DeliveryResults.isRetryable(rejected)).isTrue();
        assertThat(DeliveryResults.httpStatus(rejected)).isEqualTo(503);
    }

    @DisplayName("permanent rejection carrying a cause maps to 500")
    @Test
    void permanentRejection() {
        var rejected = (ConsumeResult.Rejected<?>) (ConsumeResult<?>) ConsumeResult
                .permanent("validation failed", new IllegalStateException());
        assertThat(DeliveryResults.isRetryable(rejected)).isFalse();
        assertThat(DeliveryResults.httpStatus(rejected)).isEqualTo(500);
    }

    @DisplayName("a permanent rejection needs no cause to map to 500")
    @Test
    void permanentWithoutCauseIs500() {
        var rejected = (ConsumeResult.Rejected<?>) (ConsumeResult<?>) ConsumeResult.permanent("policy rejected");
        assertThat(DeliveryResults.isRetryable(rejected)).isFalse();
        assertThat(DeliveryResults.httpStatus(rejected)).isEqualTo(500);
    }
}
