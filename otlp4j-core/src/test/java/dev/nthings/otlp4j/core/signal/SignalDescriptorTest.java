package dev.nthings.otlp4j.core.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.TracesData;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [SignalDescriptor]: the closed value set, its wire ordering and stability, and the
/// merge/itemCount/empty operations delegating to the model `Signal` so the two never diverge. The
/// profiles-merge parity test is the guard the report requires: the descriptor must throw the same
/// exception the model signal throws on distinct dictionaries.
class SignalDescriptorTest {

    @Test
    @DisplayName("values are the four signals in wire order, each with the right ordinal and name")
    void valuesInWireOrder() {
        assertThat(SignalDescriptor.values())
                .containsExactly(
                        SignalDescriptor.TRACES,
                        SignalDescriptor.METRICS,
                        SignalDescriptor.LOGS,
                        SignalDescriptor.PROFILES);

        assertThat(SignalDescriptor.TRACES.wireOrdinal()).isZero();
        assertThat(SignalDescriptor.METRICS.wireOrdinal()).isEqualTo(1);
        assertThat(SignalDescriptor.LOGS.wireOrdinal()).isEqualTo(2);
        assertThat(SignalDescriptor.PROFILES.wireOrdinal()).isEqualTo(3);

        assertThat(SignalDescriptor.TRACES.signalName()).isEqualTo("traces");
        assertThat(SignalDescriptor.TRACES).hasToString("traces");
        assertThat(SignalDescriptor.METRICS.signalName()).isEqualTo("metrics");
        assertThat(SignalDescriptor.LOGS.signalName()).isEqualTo("logs");
        assertThat(SignalDescriptor.PROFILES.signalName()).isEqualTo("profiles");
    }

    @Test
    @DisplayName("stability and development track the model: profiles is development, the rest stable")
    void stabilityAndDevelopment() {
        assertThat(SignalDescriptor.TRACES.stability()).isEqualTo(StabilityLevel.STABLE);
        assertThat(SignalDescriptor.METRICS.stability()).isEqualTo(StabilityLevel.STABLE);
        assertThat(SignalDescriptor.LOGS.stability()).isEqualTo(StabilityLevel.STABLE);
        assertThat(SignalDescriptor.PROFILES.stability()).isEqualTo(StabilityLevel.DEVELOPMENT);

        assertThat(SignalDescriptor.TRACES.development()).isFalse();
        assertThat(SignalDescriptor.PROFILES.development()).isTrue();
    }

    @Test
    @DisplayName("batchType matches the model batch class for each signal")
    void batchTypes() {
        assertThat(SignalDescriptor.TRACES.batchType()).isEqualTo(TracesData.class);
        assertThat(SignalDescriptor.METRICS.batchType()).isEqualTo(MetricsData.class);
        assertThat(SignalDescriptor.LOGS.batchType()).isEqualTo(LogsData.class);
        assertThat(SignalDescriptor.PROFILES.batchType()).isEqualTo(ProfilesData.class);
    }

    @Test
    @DisplayName("empty, merge and itemCount delegate to the model signal")
    void delegation() {
        assertThat(SignalDescriptor.TRACES.empty().spanCount()).isZero();

        var one = new TracesData(new TracesData(List.of()).resourceSpans());
        var merged = SignalDescriptor.TRACES.merge(List.of(one, SignalDescriptor.TRACES.empty()));
        assertThat(SignalDescriptor.TRACES.itemCount(merged))
                .isEqualTo(Signal.TRACES.itemCount(Signal.TRACES.merge(List.of(one, Signal.TRACES.empty()))));
    }

    @Test
    @DisplayName("PROFILES.merge throws the same IllegalStateException as Signal.PROFILES on "
            + "distinct dictionaries")
    void profilesMergeParity() {
        var first = new ProfilesData(List.of(), new byte[] {1});
        var second = new ProfilesData(List.of(), new byte[] {2});

        assertThatThrownBy(() -> Signal.PROFILES.merge(List.of(first, second)))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> SignalDescriptor.PROFILES.merge(List.of(first, second)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("distinct ProfilesDictionaries");
    }
}
