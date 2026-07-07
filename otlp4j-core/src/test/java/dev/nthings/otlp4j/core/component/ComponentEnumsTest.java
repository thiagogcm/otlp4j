package dev.nthings.otlp4j.core.component;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the [ComponentKind] and [StabilityLevel] enums.
class ComponentEnumsTest {

    @Test
    @DisplayName("component kinds are the five Collector kinds and round-trip through valueOf")
    void componentKinds() {
        assertThat(ComponentKind.values())
                .containsExactly(
                        ComponentKind.RECEIVER,
                        ComponentKind.PROCESSOR,
                        ComponentKind.EXPORTER,
                        ComponentKind.CONNECTOR,
                        ComponentKind.EXTENSION);
        for (var kind : ComponentKind.values()) {
            assertThat(ComponentKind.valueOf(kind.name())).isEqualTo(kind);
        }
    }

    @Test
    @DisplayName("stability levels increase in maturity and round-trip through valueOf")
    void stabilityLevels() {
        assertThat(StabilityLevel.values())
                .containsExactly(
                        StabilityLevel.DEVELOPMENT,
                        StabilityLevel.ALPHA,
                        StabilityLevel.BETA,
                        StabilityLevel.STABLE);
        for (var level : StabilityLevel.values()) {
            assertThat(StabilityLevel.valueOf(level.name())).isEqualTo(level);
        }
    }
}
