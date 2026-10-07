package dev.nthings.otlp4j.core.result;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Construction validation for [Provenance].
class ResultValueTypesTest {

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
}
