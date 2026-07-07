package dev.nthings.otlp4j.core.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [SignalPair] and the [TelemetryPayload] marker.
class SignalPairTest {

    @Test
    @DisplayName("a pair carries its from and to sides and renders them")
    void pairSides() {
        var pair = new SignalPair<>(SignalDescriptor.TRACES, SignalDescriptor.METRICS);
        assertThat(pair.from()).isEqualTo(SignalDescriptor.TRACES);
        assertThat(pair.to()).isEqualTo(SignalDescriptor.METRICS);
        assertThat(pair).hasToString("traces->metrics");
        assertThat(pair).isEqualTo(new SignalPair<>(SignalDescriptor.TRACES, SignalDescriptor.METRICS));
    }

    @Test
    @DisplayName("both sides must be non-null")
    void nullSidesRejected() {
        assertThatThrownBy(() -> new SignalPair<>(null, SignalDescriptor.METRICS))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SignalPair<>(SignalDescriptor.TRACES, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a telemetry payload reports its signal and item count")
    void telemetryPayloadMarker() {
        TelemetryPayload payload = new TelemetryPayload() {
            @Override
            public SignalDescriptor<?> signal() {
                return SignalDescriptor.LOGS;
            }

            @Override
            public long itemCount() {
                return 7L;
            }
        };
        assertThat(payload.signal()).isEqualTo(SignalDescriptor.LOGS);
        assertThat(payload.itemCount()).isEqualTo(7L);
    }
}
