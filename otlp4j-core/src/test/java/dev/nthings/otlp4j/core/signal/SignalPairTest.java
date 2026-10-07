package dev.nthings.otlp4j.core.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Payload replacement and malformed signal pairs.
class SignalPairTest {

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
        var second = TracesData.of(
                Resource.of(Attributes.builder().put("service.name", "checkout").build()),
                InstrumentationScope.of("replacement"),
                List.of(Span.builder().name("pay").build(), Span.builder().name("ship").build()));
        Payload<TracesData> payload = Payload.of(SignalDescriptor.TRACES, first);

        var replacement = payload.withData(second);
        assertThat(replacement.data().spans()).extracting(Span::name).containsExactly("pay", "ship");
        assertThat(replacement.itemCount()).isEqualTo(2);
        assertThat(replacement.routingMetadata().resourceAttribute("service.name")).contains("checkout");
        assertThat(payload.itemCount()).isZero();
        assertThat(payload.routingMetadata().resourceAttributeKeys()).isEmpty();
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
