package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("DeliveryResultStatuses")
class DeliveryResultStatusesTest {

    private static final Provenance PROV =
            new Provenance(new ComponentId("otlp", "e"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);

    private static DeliveryResult.Rejected reject(DeliveryResult result) {
        return (DeliveryResult.Rejected) result;
    }

    @DisplayName("retryable rejection maps to 503")
    @Test
    void retryableRejection() {
        var rejected = reject(DeliveryResult.retryable("backpressure", PROV));
        assertThat(DeliveryResultStatuses.isRetryable(rejected)).isTrue();
        assertThat(DeliveryResultStatuses.httpStatus(rejected)).isEqualTo(503);
    }

    @DisplayName("permanent rejection carrying a cause maps to 500")
    @Test
    void permanentRejection() {
        var rejected = reject(DeliveryResult.permanent("validation failed", new IllegalStateException(), PROV));
        assertThat(DeliveryResultStatuses.isRetryable(rejected)).isFalse();
        assertThat(DeliveryResultStatuses.httpStatus(rejected)).isEqualTo(500);
    }

    @DisplayName("retryAfterSeconds is empty without a hint")
    @Test
    void retryAfterSecondsEmptyWithoutHint() {
        var rejected = reject(DeliveryResult.retryable("backpressure", PROV));
        assertThat(DeliveryResultStatuses.retryAfterSeconds(rejected)).isEmpty();
    }

    @DisplayName("retryAfterSeconds rounds up to whole seconds")
    @Test
    void retryAfterSecondsRoundsUp() {
        var rejected = reject(DeliveryResult.throttled(Duration.ofMillis(1500), "at capacity", PROV));
        assertThat(DeliveryResultStatuses.retryAfterSeconds(rejected)).hasValue(2L);
    }

    @DisplayName("a sub-second hint floors at Retry-After: 1")
    @Test
    void subSecondHintFloorsAtOne() {
        var rejected = reject(DeliveryResult.throttled(Duration.ofMillis(50), "at capacity", PROV));
        assertThat(DeliveryResultStatuses.retryAfterSeconds(rejected)).hasValue(1L);
    }

    @DisplayName("a whole-second hint passes through unchanged")
    @Test
    void wholeSecondHintPassesThrough() {
        var rejected = reject(DeliveryResult.throttled(Duration.ofSeconds(30), "at capacity", PROV));
        assertThat(DeliveryResultStatuses.retryAfterSeconds(rejected)).hasValue(30L);
    }
}
