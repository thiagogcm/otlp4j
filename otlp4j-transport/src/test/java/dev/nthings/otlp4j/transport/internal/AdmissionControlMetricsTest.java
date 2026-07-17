package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.testing.RecordingGatewayMetrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Pins F11.4: a real registry always builds a counting instance (even uncapped), registers the
/// in-flight gauges and a rejected counter, and is closeable/idempotent; only noop-and-uncapped
/// returns the shared no-op singleton.
@DisplayName("AdmissionControl self-observability")
class AdmissionControlMetricsTest {

    private static final String REJECTED = "otlp4j.receiver.admission.rejected";
    private static final String IN_FLIGHT_REQ = "otlp4j.receiver.admission.in_flight.requests";
    private static final String IN_FLIGHT_BYTES = "otlp4j.receiver.admission.in_flight.bytes";

    @DisplayName("a capped control feeds the rejected counter and in-flight gauges")
    @Test
    void cappedControlReportsRejectionsAndInFlight() {
        var registry = new RecordingGatewayMetrics();
        var admission = AdmissionControl.of(
                OtlpReceiverConfig.builder(4317).setMaxInFlightRequests(1).build(), registry);

        var permit = admission.tryAcquire(() -> 40);
        assertThat(permit).isNotNull();
        assertThat(registry.gaugeValue(IN_FLIGHT_REQ)).isEqualTo(1);
        assertThat(registry.gaugeValue(IN_FLIGHT_BYTES)).isEqualTo(40);

        assertThat(admission.tryAcquire(() -> 10)).isNull();
        assertThat(admission.rejectedCount()).isEqualTo(1);
        assertThat(registry.counterValue(REJECTED)).isEqualTo(1);

        permit.close();
        assertThat(registry.gaugeValue(IN_FLIGHT_REQ)).isZero();
        admission.close();
    }

    @DisplayName("an uncapped control with a real registry still registers gauges and a zero rejected counter")
    @Test
    void uncappedWithRegistryStillObserves() {
        var registry = new RecordingGatewayMetrics();
        var admission = AdmissionControl.of(OtlpReceiverConfig.builder(4317).build(), registry);

        // Not the shared singleton: a per-instance counting control that observes but never rejects.
        assertThat(admission).isNotSameAs(AdmissionControl.unlimited());

        var permit = admission.tryAcquire(() -> 123);
        assertThat(permit).isNotNull();
        assertThat(registry.gaugeValue(IN_FLIGHT_REQ)).isEqualTo(1);
        assertThat(registry.gaugeValue(IN_FLIGHT_BYTES)).isEqualTo(123);
        assertThat(registry.counterValue(REJECTED)).isZero();

        permit.close();
        admission.close();
    }

    @DisplayName("uncapped with the noop registry returns the shared no-op singleton and registers nothing")
    @Test
    void uncappedWithNoopIsSharedSingleton() {
        var admission = AdmissionControl.of(OtlpReceiverConfig.builder(4317).build(), GatewayMetrics.noop());
        assertThat(admission).isSameAs(AdmissionControl.unlimited());
        // The plain of(config) overload also delegates to the noop path.
        assertThat(AdmissionControl.of(OtlpReceiverConfig.builder(4317).build())).isSameAs(AdmissionControl.unlimited());
    }

    @DisplayName("close unregisters the in-flight gauges and is idempotent")
    @Test
    void closeUnregistersGaugesIdempotently() {
        var registry = new RecordingGatewayMetrics();
        var admission = AdmissionControl.of(
                OtlpReceiverConfig.builder(4317).setMaxInFlightBytes(1000).build(), registry);

        var permit = admission.tryAcquire(() -> 50);
        assertThat(permit).isNotNull();
        assertThat(registry.gaugeValue(IN_FLIGHT_BYTES)).isEqualTo(50);

        admission.close();
        assertThat(registry.gaugeValue(IN_FLIGHT_REQ)).isZero();
        assertThat(registry.gaugeValue(IN_FLIGHT_BYTES)).isZero();
        assertThatCode(admission::close).doesNotThrowAnyException();
        permit.close();
    }
}
