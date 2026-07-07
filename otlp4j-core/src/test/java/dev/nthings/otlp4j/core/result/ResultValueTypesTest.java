package dev.nthings.otlp4j.core.result;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the result-package value types: [Provenance] and the [Disposition] and
/// [DeliveryReason] enums.
class ResultValueTypesTest {

    @Test
    @DisplayName("provenance carries the component id, kind and signal")
    void provenanceComponents() {
        var id = new ComponentId("otlp", "primary");
        var provenance = new Provenance(id, ComponentKind.RECEIVER, SignalDescriptor.TRACES);
        assertThat(provenance.componentId()).isEqualTo(id);
        assertThat(provenance.kind()).isEqualTo(ComponentKind.RECEIVER);
        assertThat(provenance.signal()).isEqualTo(SignalDescriptor.TRACES);
        assertThat(provenance)
                .isEqualTo(new Provenance(id, ComponentKind.RECEIVER, SignalDescriptor.TRACES));
    }

    @Test
    @DisplayName("provenance rejects any null component")
    void provenanceNulls() {
        var id = new ComponentId("otlp", "");
        assertThatThrownBy(() -> new Provenance(null, ComponentKind.RECEIVER, SignalDescriptor.TRACES))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Provenance(id, null, SignalDescriptor.TRACES))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Provenance(id, ComponentKind.RECEIVER, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("the disposition and reason enums round-trip through name and valueOf")
    void enumsRoundTrip() {
        for (var disposition : Disposition.values()) {
            assertThat(Disposition.valueOf(disposition.name())).isEqualTo(disposition);
        }
        for (var reason : DeliveryReason.values()) {
            assertThat(DeliveryReason.valueOf(reason.name())).isEqualTo(reason);
        }
        assertThat(Disposition.values()).containsExactly(Disposition.RETRYABLE, Disposition.PERMANENT);
        assertThat(DeliveryReason.values()).hasSize(8);
    }
}
