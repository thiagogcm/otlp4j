package dev.nthings.otlp4j.core.component;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the [ComponentKind] enum.
class ComponentEnumsTest {

    @Test
    @DisplayName("component kinds are the four Collector kinds and round-trip through valueOf")
    void componentKinds() {
        assertThat(ComponentKind.values())
                .containsExactly(
                        ComponentKind.RECEIVER,
                        ComponentKind.PROCESSOR,
                        ComponentKind.EXPORTER,
                        ComponentKind.CONNECTOR);
        for (var kind : ComponentKind.values()) {
            assertThat(ComponentKind.valueOf(kind.name())).isEqualTo(kind);
        }
    }
}
