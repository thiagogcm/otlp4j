package dev.nthings.otlp4j.core.pipeline;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Invalid pipeline identities fail before they can enter a service graph.
class PipelineIdTest {

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
