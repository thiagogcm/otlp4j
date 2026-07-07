package dev.nthings.otlp4j.core.component;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [ComponentId]: the `type[/name]` parse and render round-trip and the type
/// validation.
class ComponentIdTest {

    @Test
    @DisplayName("a bare type parses to an empty name and renders back to the bare type")
    void bareType() {
        var id = ComponentId.parse("otlp");
        assertThat(id.type()).isEqualTo("otlp");
        assertThat(id.name()).isEmpty();
        assertThat(id).hasToString("otlp");
    }

    @Test
    @DisplayName("a type/name parses on the first slash and renders back to type/name")
    void typeAndName() {
        var id = ComponentId.parse("otlp/secondary");
        assertThat(id.type()).isEqualTo("otlp");
        assertThat(id.name()).isEqualTo("secondary");
        assertThat(id).hasToString("otlp/secondary");
    }

    @Test
    @DisplayName("only the first slash splits, so a name may contain slashes")
    void firstSlashOnly() {
        var id = ComponentId.parse("otlp/a/b");
        assertThat(id.type()).isEqualTo("otlp");
        assertThat(id.name()).isEqualTo("a/b");
        assertThat(id).hasToString("otlp/a/b");
    }

    @Test
    @DisplayName("a null name is normalized to the empty string")
    void nullNameNormalized() {
        assertThat(new ComponentId("otlp", null).name()).isEmpty();
    }

    @Test
    @DisplayName("a blank type is rejected by both the constructor and parse")
    void blankTypeRejected() {
        assertThatThrownBy(() -> new ComponentId(" ", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ComponentId(null, "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ComponentId.parse("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ComponentId.parse("  ")).isInstanceOf(IllegalArgumentException.class);
    }
}
