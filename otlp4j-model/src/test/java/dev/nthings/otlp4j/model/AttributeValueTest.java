package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.AttributeValue.ArrayValue;
import dev.nthings.otlp4j.model.AttributeValue.BoolValue;
import dev.nthings.otlp4j.model.AttributeValue.BytesValue;
import dev.nthings.otlp4j.model.AttributeValue.DoubleValue;
import dev.nthings.otlp4j.model.AttributeValue.Empty;
import dev.nthings.otlp4j.model.AttributeValue.KeyValueListValue;
import dev.nthings.otlp4j.model.AttributeValue.LongValue;
import dev.nthings.otlp4j.model.AttributeValue.StringValue;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [AttributeValue] — every variant, factory and the defensive-copy contract of
/// [BytesValue], plus nested-structure deep equality.
@DisplayName("AttributeValue")
class AttributeValueTest {

    // --- BytesValue: the priority — defensive copying both ways -----------------------------

    @DisplayName("BytesValue defensively copies the constructor input")
    @Test
    void bytesValueDefensivelyCopiesTheInputArrayOnConstruction() {
        byte[] source = {1, 2, 3};
        var value = new BytesValue(source);

        source[0] = 99;

        assertThat(value.value())
                .as("mutating the constructor input must not affect the wrapped array")
                .containsExactly(1, 2, 3);
    }

    @DisplayName("BytesValue.value() returns a fresh copy each call")
    @Test
    void bytesValueAccessorReturnsAFreshCopyEachCall() {
        var value = new BytesValue(new byte[] {1, 2, 3});

        byte[] first = value.value();
        first[0] = 99;

        assertThat(value.value())
                .as("mutating the array returned by value() must not affect the record")
                .containsExactly(1, 2, 3);
        assertThat(value.value()).isNotSameAs(value.value());
    }

    @DisplayName("BytesValue equals compares content not reference")
    @Test
    void bytesValueEqualsComparesContentNotReference() {
        var a = new BytesValue(new byte[] {1, 2, 3});
        var b = new BytesValue(new byte[] {1, 2, 3});
        var c = new BytesValue(new byte[] {1, 2, 4});

        assertThat(a).isEqualTo(b).isNotEqualTo(c);
        assertThat(a).isNotEqualTo("not a BytesValue");
    }

    @DisplayName("BytesValue hashCode is content-based")
    @Test
    void bytesValueHashCodeIsContentBased() {
        var a = new BytesValue(new byte[] {4, 5, 6});
        var b = new BytesValue(new byte[] {4, 5, 6});

        assertThat(a).hasSameHashCodeAs(b);
    }

    @DisplayName("BytesValue toString reports the byte count")
    @Test
    void bytesValueToStringReportsTheByteCount() {
        assertThat(new BytesValue(new byte[] {1, 2, 3, 4}).toString())
                .isEqualTo("BytesValue[4 bytes]");
        assertThat(new BytesValue(new byte[0]).toString()).isEqualTo("BytesValue[0 bytes]");
    }

    // --- the remaining scalar variants ------------------------------------------------------

    @DisplayName("scalar variants expose their wrapped value")
    @Test
    void scalarVariantsExposeTheirWrappedValue() {
        assertThat(new StringValue("hello").value()).isEqualTo("hello");
        assertThat(new BoolValue(true).value()).isTrue();
        assertThat(new LongValue(42L).value()).isEqualTo(42L);
        assertThat(new DoubleValue(3.5).value()).isEqualTo(3.5);
        assertThat(new Empty()).isEqualTo(AttributeValue.EMPTY);
    }

    @DisplayName("ArrayValue and KeyValueListValue expose their collections")
    @Test
    void arrayValueAndKeyValueListValueExposeTheirWrappedCollections() {
        var array = new ArrayValue(List.of(AttributeValue.of("a"), AttributeValue.of(1L)));
        assertThat(array.values()).containsExactly(AttributeValue.of("a"), AttributeValue.of(1L));

        var map = new KeyValueListValue(Map.of("k", AttributeValue.of("v")));
        assertThat(map.values()).containsEntry("k", AttributeValue.of("v"));
    }

    // --- static factories -------------------------------------------------------------------

    @DisplayName("of produces the matching variant per scalar overload")
    @Test
    void ofProducesTheMatchingVariantForEachScalarOverload() {
        assertThat(AttributeValue.of("s")).isEqualTo(new StringValue("s"));
        assertThat(AttributeValue.of(true)).isEqualTo(new BoolValue(true));
        assertThat(AttributeValue.of(7L)).isEqualTo(new LongValue(7L));
        assertThat(AttributeValue.of(1.25)).isEqualTo(new DoubleValue(1.25));
        assertThat(AttributeValue.of(new byte[] {9})).isEqualTo(new BytesValue(new byte[] {9}));
    }

    @DisplayName("empty() and EMPTY return the shared Empty singleton")
    @Test
    void emptyAndEMPTYReturnTheSharedEmptySingleton() {
        assertThat(AttributeValue.empty()).isSameAs(AttributeValue.EMPTY);
        assertThat(AttributeValue.EMPTY).isInstanceOf(Empty.class);
    }

    @DisplayName("of(List) copies its input so later mutation does not leak in")
    @Test
    void ofListCopiesItsInputSoLaterMutationDoesNotLeakIn() {
        var source = new ArrayList<AttributeValue>();
        source.add(AttributeValue.of("first"));
        var value = (ArrayValue) AttributeValue.of(source);

        source.add(AttributeValue.of("second"));

        assertThat(value.values()).containsExactly(AttributeValue.of("first"));
    }

    @DisplayName("of(Map) copies its input and preserves iteration order")
    @Test
    void ofMapCopiesItsInputAndPreservesIterationOrder() {
        var source = new LinkedHashMap<String, AttributeValue>();
        source.put("z", AttributeValue.of("1"));
        source.put("a", AttributeValue.of("2"));
        source.put("m", AttributeValue.of("3"));
        var value = (KeyValueListValue) AttributeValue.of(source);

        source.put("leaked", AttributeValue.of("4"));

        assertThat(value.values()).containsOnlyKeys("z", "a", "m");
        assertThat(value.values().keySet())
                .as("of(Map) must preserve the source iteration order")
                .containsExactly("z", "a", "m");
    }

    // --- nested structures ------------------------------------------------------------------

    @DisplayName("nested ArrayValue of KeyValueListValue of bytes supports deep equality")
    @Test
    void nestedArrayOfKeyValueListOfBytesSupportsDeepEquality() {
        AttributeValue nestedA = new ArrayValue(List.of(
                new KeyValueListValue(Map.of("payload", new BytesValue(new byte[] {1, 2, 3})))));
        AttributeValue nestedB = new ArrayValue(List.of(
                new KeyValueListValue(Map.of("payload", new BytesValue(new byte[] {1, 2, 3})))));
        AttributeValue different = new ArrayValue(List.of(
                new KeyValueListValue(Map.of("payload", new BytesValue(new byte[] {9})))));

        assertThat(nestedA).isEqualTo(nestedB).isNotEqualTo(different);
    }
}
