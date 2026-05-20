package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/// Unit tests for [Attributes] — the builder overloads, the [Attributes#of(Map)] copy/order/
/// empty-singleton contract, and every accessor including the `getString` type-mismatch path.
class AttributesTest {

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

    @Test
    void builderPreservesInsertionOrder() {
        var attributes = Attributes.builder()
                .put("z", "1")
                .put("a", "2")
                .put("m", "3")
                .build();

        assertThat(attributes.keys()).containsExactly("z", "a", "m");
    }

    @Test
    void ofMapCopiesItsInputSoLaterMutationDoesNotLeakIn() {
        var source = new LinkedHashMap<String, AttributeValue>();
        source.put("key", AttributeValue.of("value"));
        var attributes = Attributes.of(source);

        source.put("leaked", AttributeValue.of("nope"));

        assertThat(attributes.contains("leaked")).isFalse();
        assertThat(attributes.keys()).containsExactly("key");
    }

    @Test
    void ofMapPreservesIterationOrder() {
        var source = new LinkedHashMap<String, AttributeValue>();
        source.put("third", AttributeValue.of("3"));
        source.put("first", AttributeValue.of("1"));
        source.put("second", AttributeValue.of("2"));

        assertThat(Attributes.of(source).keys()).containsExactly("third", "first", "second");
    }

    @Test
    void ofEmptyMapShortCircuitsToTheEmptySingleton() {
        assertThat(Attributes.of(Map.of()))
                .as("the empty-map short-circuit must return the EMPTY singleton")
                .isSameAs(Attributes.empty());
    }

    @Test
    void getReturnsTheValueWhenTheKeyExistsAndNullOtherwise() {
        var attributes = Attributes.builder().put("present", "yes").build();

        assertThat(attributes.get("present")).isEqualTo(AttributeValue.of("yes"));
        assertThat(attributes.get("absent")).isNull();
    }

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

    @Test
    void containsReflectsKeyPresence() {
        var attributes = Attributes.builder().put("here", "v").build();

        assertThat(attributes.contains("here")).isTrue();
        assertThat(attributes.contains("missing")).isFalse();
    }

    @Test
    void asMapReturnsAnUnmodifiableView() {
        var attributes = Attributes.builder().put("k", "v").build();

        assertThatThrownBy(() -> attributes.asMap().put("x", AttributeValue.of("y")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(attributes.asMap()).containsEntry("k", AttributeValue.of("v"));
    }

    @Test
    void sizeAndIsEmptyReflectContents() {
        assertThat(Attributes.empty().isEmpty()).isTrue();
        assertThat(Attributes.empty().size()).isZero();

        var populated = Attributes.builder().put("a", "1").put("b", "2").build();
        assertThat(populated.isEmpty()).isFalse();
        assertThat(populated.size()).isEqualTo(2);
    }

    @Test
    void equalsAndHashCodeAreContentBased() {
        var a = Attributes.builder().put("k", "v").build();
        var b = Attributes.builder().put("k", "v").build();
        var different = Attributes.builder().put("k", "other").build();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(different);
        assertThat(a).isNotEqualTo("not attributes");
    }

    @Test
    void toStringIncludesTheUnderlyingMap() {
        assertThat(Attributes.builder().put("k", "v").build().toString())
                .startsWith("Attributes")
                .contains("k");
    }

    @Test
    void keysReflectsAbsenceForTheEmptySingleton() {
        assertThat(Attributes.empty().keys()).isEmpty();
        assertThat(Attributes.empty().get("anything")).isNull();
    }
}
