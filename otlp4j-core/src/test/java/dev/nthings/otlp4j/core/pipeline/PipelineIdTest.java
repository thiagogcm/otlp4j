package dev.nthings.otlp4j.core.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [PipelineId]: the default and named forms and the `signal[/name]` rendering.
class PipelineIdTest {

    @Test
    @DisplayName("the default pipeline renders as the bare signal name")
    void defaultPipeline() {
        var id = PipelineId.of(SignalDescriptor.TRACES);
        assertThat(id.signal()).isEqualTo(SignalDescriptor.TRACES);
        assertThat(id.name()).isEmpty();
        assertThat(id).hasToString("traces");
    }

    @Test
    @DisplayName("a named pipeline renders as signal/name")
    void namedPipeline() {
        var id = PipelineId.of(SignalDescriptor.LOGS, "audit");
        assertThat(id.name()).contains("audit");
        assertThat(id).hasToString("logs/audit");
        assertThat(id).isEqualTo(new PipelineId(SignalDescriptor.LOGS, Optional.of("audit")));
    }

    @Test
    @DisplayName("the signal is required and a present name must be non-blank")
    void validation() {
        assertThatThrownBy(() -> new PipelineId(null, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PipelineId(SignalDescriptor.TRACES, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PipelineId.of(SignalDescriptor.TRACES, " "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
