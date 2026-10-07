package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Attribute updates and immutable snapshots of source maps.
@DisplayName("Attributes")
class AttributesTest {

    @DisplayName("builder supports every put overload")
    @Test
    void builderSupportsEveryPutOverload() {
        var attributes = Attributes.builder()
                .put("explicit", AttributeValue.of("v"))
                .put("string", "s")
                .put("long", 10L)
                .put("double", 2.5)
                .put("bool", true)
                .build();

        assertThat(attributes.get("explicit")).isEqualTo(AttributeValue.of("v"));
        assertThat(attributes.get("string")).isEqualTo(AttributeValue.of("s"));
        assertThat(attributes.get("long")).isEqualTo(AttributeValue.of(10L));
        assertThat(attributes.get("double")).isEqualTo(AttributeValue.of(2.5));
        assertThat(attributes.get("bool")).isEqualTo(AttributeValue.of(true));
        assertThat(attributes.size()).isEqualTo(5);
    }

    @DisplayName("toBuilder pre-populates, then put adds and overrides")
    @Test
    void toBuilderPrePopulatesThenPutAddsAndOverrides() {
        var base = Attributes.builder().put("keep", "v").put("override", "old").build();

        var derived = base.toBuilder()
                .put("override", "new")
                .put("added", "x")
                .build();

        assertThat(derived.getString("keep")).isEqualTo("v");
        assertThat(derived.getString("override")).isEqualTo("new");
        assertThat(derived.getString("added")).isEqualTo("x");
        assertThat(base.getString("override"))
                .as("toBuilder must not mutate the source attributes")
                .isEqualTo("old");
    }

    @DisplayName("with adds or replaces one attribute across every overload, leaving the source intact")
    @Test
    void withAddsOrReplacesOneAttribute() {
        var base = Attributes.builder().put("keep", "v").put("override", "old").build();

        var derived = base.with("override", "new")
                .with("explicit", AttributeValue.of("e"))
                .with("long", 10L)
                .with("double", 2.5)
                .with("bool", true);

        assertThat(derived.getString("keep")).isEqualTo("v");
        assertThat(derived.getString("override")).isEqualTo("new");
        assertThat(derived.get("explicit")).isEqualTo(AttributeValue.of("e"));
        assertThat(derived.get("long")).isEqualTo(AttributeValue.of(10L));
        assertThat(derived.get("double")).isEqualTo(AttributeValue.of(2.5));
        assertThat(derived.get("bool")).isEqualTo(AttributeValue.of(true));
        assertThat(base.getString("override"))
                .as("with must not mutate the source attributes")
                .isEqualTo("old");
        assertThat(base.contains("explicit")).isFalse();
    }

    @DisplayName("putAll copies from Attributes and from a Map, overwriting keys")
    @Test
    void putAllCopiesFromAttributesAndFromAMapOverwritingKeys() {
        var fromAttributes = Attributes.builder().put("a", "1").put("shared", "from-attrs").build();
        var fromMap = Map.of("b", AttributeValue.of("2"), "shared", AttributeValue.of("from-map"));

        var merged = Attributes.builder()
                .put("shared", "original")
                .putAll(fromAttributes)
                .putAll(fromMap)
                .build();

        assertThat(merged.getString("a")).isEqualTo("1");
        assertThat(merged.getString("b")).isEqualTo("2");
        assertThat(merged.getString("shared")).isEqualTo("from-map");
    }

    @DisplayName("builder preserves insertion order")
    @Test
    void builderPreservesInsertionOrder() {
        var attributes = Attributes.builder()
                .put("z", "1")
                .put("a", "2")
                .put("m", "3")
                .build();

        assertThat(attributes.keys()).containsExactly("z", "a", "m");
    }

    @DisplayName("of(Map) copies its input so later mutation does not leak in")
    @Test
    void ofMapCopiesItsInputSoLaterMutationDoesNotLeakIn() {
        var source = new LinkedHashMap<String, AttributeValue>();
        source.put("key", AttributeValue.of("value"));
        var attributes = Attributes.of(source);

        source.put("leaked", AttributeValue.of("nope"));

        assertThat(attributes.contains("leaked")).isFalse();
        assertThat(attributes.keys()).containsExactly("key");
    }

    @DisplayName("of(Map) preserves iteration order")
    @Test
    void ofMapPreservesIterationOrder() {
        var source = new LinkedHashMap<String, AttributeValue>();
        source.put("third", AttributeValue.of("3"));
        source.put("first", AttributeValue.of("1"));
        source.put("second", AttributeValue.of("2"));

        assertThat(Attributes.of(source).keys()).containsExactly("third", "first", "second");
    }

    @DisplayName("getString returns null for absent or non-string values")
    @Test
    void getStringReturnsTheStringWhenPresentAndNullWhenAbsentOrNotAString() {
        var attributes = Attributes.builder()
                .put("string", "hello")
                .put("number", 5L)
                .build();

        assertThat(attributes.getString("string")).isEqualTo("hello");
        assertThat(attributes.getString("absent")).isNull();
        assertThat(attributes.getString("number"))
                .as("getString must return null for a present-but-non-string value")
                .isNull();
    }

    @DisplayName("asMap returns an unmodifiable view")
    @Test
    void asMapReturnsAnUnmodifiableView() {
        var attributes = Attributes.builder().put("k", "v").build();

        assertThatThrownBy(() -> attributes.asMap().put("x", AttributeValue.of("y")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(attributes.asMap()).containsEntry("k", AttributeValue.of("v"));
    }

    @DisplayName("size and isEmpty reflect contents")
    @Test
    void sizeAndIsEmptyReflectContents() {
        assertThat(Attributes.empty().isEmpty()).isTrue();
        assertThat(Attributes.empty().size()).isZero();

        var populated = Attributes.builder().put("a", "1").put("b", "2").build();
        assertThat(populated.isEmpty()).isFalse();
        assertThat(populated.size()).isEqualTo(2);
    }

    @DisplayName("equals and hashCode are content-based")
    @Test
    void equalsAndHashCodeAreContentBased() {
        var a = Attributes.builder().put("k", "v").build();
        var b = Attributes.builder().put("k", "v").build();
        var different = Attributes.builder().put("k", "other").build();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(different);
        assertThat(a).isNotEqualTo("not attributes");
    }
}
