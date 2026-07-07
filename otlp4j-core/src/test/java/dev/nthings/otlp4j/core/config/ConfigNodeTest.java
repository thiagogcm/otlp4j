package dev.nthings.otlp4j.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [ConfigNode]: the typed, key-tracking accessors, the duration grammar, the nested
/// child navigation, and the unread-key report that drives unknown-key diagnostics.
class ConfigNodeTest {

    @Test
    @DisplayName("empty and of(empty) share the empty node; of(null) is rejected")
    void construction() {
        assertThat(ConfigNode.empty().keys()).isEmpty();
        assertThat(ConfigNode.of(Map.of())).isSameAs(ConfigNode.empty());
        assertThatThrownBy(() -> ConfigNode.of(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("string reads scalars and rejects collections; absent yields empty")
    void stringAccessor() {
        var node = ConfigNode.of(map(
                "s", "hello", "n", 5, "b", true, "list", List.of("a"), "sub", Map.of("x", 1)));
        assertThat(node.string("s")).contains("hello");
        assertThat(node.string("n")).contains("5");
        assertThat(node.string("b")).contains("true");
        assertThat(node.string("list")).isEmpty();
        assertThat(node.string("sub")).isEmpty();
        assertThat(node.string("absent")).isEmpty();
    }

    @Test
    @DisplayName("int and long read numbers and numeric strings, defaulting when absent")
    void numericAccessors() {
        var node = ConfigNode.of(map("i", 7, "istr", "42", "l", 9L, "lstr", "100"));
        assertThat(node.intValue("i", -1)).isEqualTo(7);
        assertThat(node.intValue("istr", -1)).isEqualTo(42);
        assertThat(node.intValue("absent", -1)).isEqualTo(-1);
        assertThat(node.longValue("l", -1L)).isEqualTo(9L);
        assertThat(node.longValue("lstr", -1L)).isEqualTo(100L);
        assertThat(node.longValue("absent", -1L)).isEqualTo(-1L);
    }

    @Test
    @DisplayName("a malformed numeric value is rejected, naming the key")
    void malformedNumbers() {
        var node = ConfigNode.of(map("i", "notanint", "l", "notalong"));
        assertThatThrownBy(() -> node.intValue("i", 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("'i'");
        assertThatThrownBy(() -> node.longValue("l", 0L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("'l'");
    }

    @Test
    @DisplayName("bool reads booleans and true/false strings, and rejects other values")
    void boolAccessor() {
        var node = ConfigNode.of(map("t", true, "fstr", "FALSE", "tstr", "true", "bad", "yes"));
        assertThat(node.bool("t", false)).isTrue();
        assertThat(node.bool("fstr", true)).isFalse();
        assertThat(node.bool("tstr", false)).isTrue();
        assertThat(node.bool("absent", true)).isTrue();
        assertThatThrownBy(() -> node.bool("bad", false)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("duration parses Collector token grammar, compound tokens, and ISO-8601")
    void durationAccessor() {
        var node = ConfigNode.of(map(
                "a", "1s", "b", "500ms", "c", "1m30s", "d", "250us", "e", "2h", "f", "PT3S"));
        assertThat(node.duration("a", Duration.ZERO)).isEqualTo(Duration.ofSeconds(1));
        assertThat(node.duration("b", Duration.ZERO)).isEqualTo(Duration.ofMillis(500));
        assertThat(node.duration("c", Duration.ZERO)).isEqualTo(Duration.ofSeconds(90));
        assertThat(node.duration("d", Duration.ZERO)).isEqualTo(Duration.ofNanos(250_000));
        assertThat(node.duration("e", Duration.ZERO)).isEqualTo(Duration.ofHours(2));
        assertThat(node.duration("f", Duration.ZERO)).isEqualTo(Duration.ofSeconds(3));
        assertThat(node.duration("absent", Duration.ofSeconds(9))).isEqualTo(Duration.ofSeconds(9));
    }

    @Test
    @DisplayName("a malformed duration is rejected, naming the key")
    void malformedDuration() {
        var node = ConfigNode.of(map("bad", "5 furlongs", "empty", ""));
        assertThatThrownBy(() -> node.duration("bad", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("'bad'");
        assertThatThrownBy(() -> node.duration("empty", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("stringList reads a list, wraps a scalar, and yields empty when absent")
    void stringListAccessor() {
        var node = ConfigNode.of(map("list", List.of("a", "b"), "scalar", "solo"));
        assertThat(node.stringList("list")).containsExactly("a", "b");
        assertThat(node.stringList("scalar")).containsExactly("solo");
        assertThat(node.stringList("absent")).isEmpty();
    }

    @Test
    @DisplayName("child navigates a nested map, an empty (null) value, and rejects scalars")
    void childAccessor() {
        var raw = new LinkedHashMap<String, Object>();
        raw.put("sub", Map.of("k", "v"));
        raw.put("nullish", null);
        raw.put("scalar", "x");
        var node = ConfigNode.of(raw);

        assertThat(node.child("sub")).isPresent();
        assertThat(node.child("sub").orElseThrow().string("k")).contains("v");
        assertThat(node.child("nullish")).isPresent();
        assertThat(node.child("nullish").orElseThrow().keys()).isEmpty();
        assertThat(node.child("scalar")).isEmpty();
        assertThat(node.child("absent")).isEmpty();
    }

    @Test
    @DisplayName("unreadKeys reports exactly the keys no accessor touched")
    void unreadKeysTracksReads() {
        var node = ConfigNode.of(map("read", "1", "alsoRead", "2", "untouched", "3"));
        assertThat(node.keys()).containsExactlyInAnyOrder("read", "alsoRead", "untouched");

        node.string("read");
        node.intValue("alsoRead", 0);
        assertThat(node.unreadKeys()).containsExactly("untouched");

        node.string("untouched");
        assertThat(node.unreadKeys()).isEmpty();
    }

    private static Map<String, Object> map(Object... pairs) {
        var raw = new LinkedHashMap<String, Object>();
        var flat = new ArrayList<>(List.of(pairs));
        for (var i = 0; i < flat.size(); i += 2) {
            raw.put((String) flat.get(i), flat.get(i + 1));
        }
        return raw;
    }
}
