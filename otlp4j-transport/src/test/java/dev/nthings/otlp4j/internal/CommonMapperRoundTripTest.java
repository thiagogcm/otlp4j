package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import io.opentelemetry.proto.common.v1.AnyValue;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/// Exhaustive mapper-unit round-trip coverage for [CommonMapper] — every `AttributeValue`
/// variant (including nested and empty), the `Attributes` short-circuit, the id hex helpers and
/// the resource/scope mappers, all with no gRPC layer in the loop.
class CommonMapperRoundTripTest {

    static Stream<AttributeValue> everyAttributeValueVariant() {
        return Stream.of(
                AttributeValue.of("a string"),
                AttributeValue.of(""),
                AttributeValue.of(true),
                AttributeValue.of(false),
                AttributeValue.of(42L),
                AttributeValue.of(-7L),
                AttributeValue.of(3.14),
                AttributeValue.of(new byte[] {1, 2, 3, 4}),
                AttributeValue.of(new byte[0]),
                AttributeValue.of(List.of(
                        AttributeValue.of("nested"),
                        AttributeValue.of(1L),
                        AttributeValue.of(true))),
                AttributeValue.of(Map.of("k1", AttributeValue.of("v1"), "k2", AttributeValue.of(2L))),
                AttributeValue.empty(),
                // two levels deep: ArrayValue of KeyValueListValue of BytesValue
                AttributeValue.of(List.of(AttributeValue.of(
                        Map.of("bytes", AttributeValue.of(new byte[] {9, 8, 7}))))));
    }

    @ParameterizedTest
    @MethodSource("everyAttributeValueVariant")
    void roundTripsEveryAttributeValueVariant(AttributeValue value) {
        assertThat(CommonMapper.attributeValue(CommonMapper.toAnyValue(value)))
                .as("attributeValue(toAnyValue(x)) must equal x for every variant")
                .isEqualTo(value);
    }

    @Test
    void toAnyValueOfEmptyMapsToTheDefaultAnyValueInstance() {
        assertThat(CommonMapper.toAnyValue(AttributeValue.empty()))
                .isEqualTo(AnyValue.getDefaultInstance());
    }

    @Test
    void valueNotSetAnyValueDecodesToEmpty() {
        // An AnyValue with no oneof case set is VALUE_NOT_SET.
        assertThat(CommonMapper.attributeValue(AnyValue.getDefaultInstance()))
                .as("VALUE_NOT_SET must surface as AttributeValue.empty()")
                .isEqualTo(AttributeValue.empty());
    }

    @Test
    void emptyAttributesShortCircuitsToTheSharedEmptyInstance() {
        var keyValues = CommonMapper.toKeyValues(Attributes.empty());
        assertThat(keyValues).isEmpty();
        assertThat(CommonMapper.attributes(keyValues))
                .as("an empty KeyValue list must take the isEmpty() short-circuit")
                .isEqualTo(Attributes.empty());
    }

    @Test
    void populatedAttributesRoundTrip() {
        var attributes = Attributes.builder()
                .put("str", "s")
                .put("long", 5L)
                .put("bool", true)
                .put("double", 1.5)
                .build();
        assertThat(CommonMapper.attributes(CommonMapper.toKeyValues(attributes)))
                .isEqualTo(attributes);
    }

    @Test
    void hexAndBytesRoundTripAnEmptyId() {
        assertThat(CommonMapper.hex(ByteString.EMPTY)).isEmpty();
        assertThat(CommonMapper.bytes("")).isEqualTo(ByteString.EMPTY);
        assertThat(CommonMapper.hex(CommonMapper.bytes(""))).isEmpty();
    }

    @Test
    void hexAndBytesRoundTripALowercaseHexId() {
        var id = "0102030405060708090a0b0c0d0e0f10";
        assertThat(CommonMapper.hex(CommonMapper.bytes(id)))
                .as("a lowercase-hex id must survive the bytes/hex round-trip")
                .isEqualTo(id);
    }

    @Test
    void resourceRoundTrips() {
        var resource = new Resource(
                Attributes.builder().put("service.name", "checkout").put("k", 1L).build(), 9);
        assertThat(CommonMapper.resource(CommonMapper.toProtoResource(resource)))
                .isEqualTo(resource);
    }

    @Test
    void emptyResourceRoundTrips() {
        assertThat(CommonMapper.resource(CommonMapper.toProtoResource(Resource.EMPTY)))
                .isEqualTo(Resource.EMPTY);
    }

    @Test
    void scopeRoundTrips() {
        var scope = new InstrumentationScope(
                "otlp4j-test", "1.2.3", Attributes.builder().put("a", true).build(), 4);
        assertThat(CommonMapper.scope(CommonMapper.toProtoScope(scope))).isEqualTo(scope);
    }

    @Test
    void emptyScopeRoundTrips() {
        assertThat(CommonMapper.scope(CommonMapper.toProtoScope(InstrumentationScope.EMPTY)))
                .isEqualTo(InstrumentationScope.EMPTY);
    }
}
