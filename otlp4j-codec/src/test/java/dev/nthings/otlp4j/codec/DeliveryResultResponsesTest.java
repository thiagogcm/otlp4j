package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("DeliveryResultResponses")
class DeliveryResultResponsesTest {

    private static final Provenance PROV =
            new Provenance(new ComponentId("otlp", "e"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);

    @DisplayName("accepted leaves partial_success unset on every signal")
    @Test
    void acceptedLeavesPartialUnset() {
        var accepted = DeliveryResult.accepted(PROV);
        assertThat(DeliveryResultResponses.traces(accepted).hasPartialSuccess()).isFalse();
        assertThat(DeliveryResultResponses.metrics(accepted).hasPartialSuccess()).isFalse();
        assertThat(DeliveryResultResponses.logs(accepted).hasPartialSuccess()).isFalse();
        assertThat(DeliveryResultResponses.profiles(accepted).hasPartialSuccess()).isFalse();
    }

    @DisplayName("partial carries the rejected count and message per signal")
    @Test
    void partialCarriesCountAndMessage() {
        var partial = DeliveryResult.partial(3, "dropped", PROV);

        var traces = DeliveryResultResponses.traces(partial).getPartialSuccess();
        assertThat(traces.getRejectedSpans()).isEqualTo(3);
        assertThat(traces.getErrorMessage()).isEqualTo("dropped");

        assertThat(DeliveryResultResponses.metrics(partial).getPartialSuccess().getRejectedDataPoints())
                .isEqualTo(3);
        assertThat(DeliveryResultResponses.logs(partial).getPartialSuccess().getRejectedLogRecords())
                .isEqualTo(3);
        assertThat(DeliveryResultResponses.profiles(partial).getPartialSuccess().getRejectedProfiles())
                .isEqualTo(3);
    }

    @DisplayName("a rejection is never a response message on any signal")
    @Test
    void rejectionIsNeverAResponse() {
        var rejected = DeliveryResult.permanent("nope", PROV);
        assertThatThrownBy(() -> DeliveryResultResponses.traces(rejected))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> DeliveryResultResponses.metrics(rejected))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> DeliveryResultResponses.logs(rejected))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> DeliveryResultResponses.profiles(rejected))
                .isInstanceOf(IllegalStateException.class);
    }
}
