package dev.nthings.otlp4j.core.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TracesData;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [SignalPair] and the typed [Payload] contract.
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
    @DisplayName("a payload is typed and replacements recompute derived values")
    void typedPayload() {
        var first = new TracesData(List.of());
        var second = new TracesData(List.of());
        Payload<TracesData> payload = Payload.of(SignalDescriptor.TRACES, first);

        assertThat(payload.signal()).isSameAs(SignalDescriptor.TRACES);
        assertThat(payload.data()).isSameAs(first);
        assertThat(payload.itemCount()).isZero();
        assertThat(payload.routingMetadata().resourceAttributeKeys()).isEmpty();
        assertThat(payload.withData(first)).isSameAs(payload);
        assertThat(payload.withData(second).data()).isSameAs(second);
    }

    @Test
    @DisplayName("a payload rejects null signal and data")
    void payloadNullsRejected() {
        assertThatThrownBy(() -> Payload.of(null, new TracesData(List.of())))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Payload.of(SignalDescriptor.TRACES, null))
                .isInstanceOf(NullPointerException.class);
    }
}
