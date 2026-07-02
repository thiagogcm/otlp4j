package dev.nthings.otlp4j.transport.common;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.config.ServerConfig;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("AdmissionControl")
class AdmissionControlTest {

    @DisplayName("request cap admits up to the limit, rejects beyond, and recovers on release")
    @Test
    void requestCapAdmitsAndRecovers() {
        var admission = AdmissionControl.of(
                ServerConfig.builder().setMaxInFlightRequests(2).build());

        var first = admission.tryAcquire(10);
        var second = admission.tryAcquire(10);
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(admission.inFlightRequests()).isEqualTo(2);

        assertThat(admission.tryAcquire(10)).isNull();
        assertThat(admission.rejectedCount()).isEqualTo(1);

        first.close();
        assertThat(admission.inFlightRequests()).isEqualTo(1);
        var third = admission.tryAcquire(10);
        assertThat(third).isNotNull();
        second.close();
        third.close();
        assertThat(admission.inFlightRequests()).isZero();
        assertThat(admission.inFlightBytes()).isZero();
    }

    @DisplayName("byte cap rejects a request that would exceed it while others are in flight")
    @Test
    void byteCapRejectsOvershoot() {
        var admission = AdmissionControl.of(
                ServerConfig.builder().setMaxInFlightBytes(100).build());

        var first = admission.tryAcquire(60);
        assertThat(first).isNotNull();
        assertThat(admission.inFlightBytes()).isEqualTo(60);

        assertThat(admission.tryAcquire(50)).isNull();
        assertThat(admission.inFlightBytes()).isEqualTo(60);
        assertThat(admission.rejectedCount()).isEqualTo(1);

        var second = admission.tryAcquire(40);
        assertThat(second).isNotNull();
        first.close();
        second.close();
        assertThat(admission.inFlightBytes()).isZero();
    }

    @DisplayName("progress guarantee: an oversized request is admitted when nothing is in flight")
    @Test
    void oversizedRequestAdmittedWhenIdle() {
        var admission = AdmissionControl.of(
                ServerConfig.builder().setMaxInFlightBytes(100).build());

        var oversized = admission.tryAcquire(10_000);
        assertThat(oversized).isNotNull();
        assertThat(admission.inFlightBytes()).isEqualTo(10_000);

        // While the oversized request is in flight, further requests are rejected.
        assertThat(admission.tryAcquire(1)).isNull();

        oversized.close();
        assertThat(admission.inFlightBytes()).isZero();
        var next = admission.tryAcquire(1);
        assertThat(next).isNotNull();
        next.close();
    }

    @DisplayName("permit close is idempotent")
    @Test
    void permitCloseIsIdempotent() {
        var admission = AdmissionControl.of(
                ServerConfig.builder().setMaxInFlightRequests(1).setMaxInFlightBytes(100).build());

        var permit = admission.tryAcquire(40);
        assertThat(permit).isNotNull();
        permit.close();
        permit.close();
        permit.close();
        assertThat(admission.inFlightRequests()).isZero();
        assertThat(admission.inFlightBytes()).isZero();
    }

    @DisplayName("both limits disabled resolves to the shared unlimited no-op instance")
    @Test
    void disabledLimitsAreUnlimited() {
        var admission = AdmissionControl.of(ServerConfig.builder().build());
        assertThat(admission).isSameAs(AdmissionControl.unlimited());

        for (var i = 0; i < 10_000; i++) {
            var permit = admission.tryAcquire(Long.MAX_VALUE / 2);
            assertThat(permit).isNotNull();
            permit.close();
        }
        assertThat(admission.inFlightRequests()).isZero();
        assertThat(admission.inFlightBytes()).isZero();
        assertThat(admission.rejectedCount()).isZero();
    }

    @DisplayName("the retry-after hint mirrors the config")
    @Test
    void retryAfterHintMirrorsConfig() {
        var without = AdmissionControl.of(ServerConfig.builder().setMaxInFlightRequests(1).build());
        assertThat(without.retryAfterHint()).isNull();

        var with = AdmissionControl.of(ServerConfig.builder()
                .setMaxInFlightRequests(1)
                .setThrottleRetryAfter(Duration.ofSeconds(3))
                .build());
        assertThat(with.retryAfterHint()).isEqualTo(Duration.ofSeconds(3));
    }
}
