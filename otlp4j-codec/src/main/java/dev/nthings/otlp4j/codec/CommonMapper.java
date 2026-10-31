package dev.nthings.otlp4j.codec;

import com.google.protobuf.ByteString;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.IntFunction;
import org.jspecify.annotations.Nullable;

/// Maps OTLP common/resource types between proto and domain, in both directions.
///
/// Internal. Part of the transport layer; not public API. Generated proto
/// classes are confined to this package and never escape into domain or high-level layers.
final class CommonMapper {

    private static final HexFormat HEX = HexFormat.of();

    /// The string lookup of a signal without a `ProfilesDictionary`, whose references mean nothing:
    /// OTLP requires such a signal to process them as absent or empty.
    private static final IntFunction<@Nullable String> NO_STRINGS = index -> null;

    private CommonMapper() {}

    // --- proto -> domain ---

    /// Renders a trace/span/profile ID as lowercase-hex.
    public static String hex(ByteString bytes) {
        return bytes.isEmpty() ? "" : HEX.formatHex(bytes.toByteArray());
    }

    /// Interprets an OTLP export partial-success as a [DeliveryResult], attributing the outcome to
    /// `provenance`. Shared by all four signal mappers.
    ///
    /// A present partial-success block is always represented as a partial result, including a zero
    /// rejected count carrying a server warning. Partial success is terminal and is never retried.
    ///
    /// @param hasPartialSuccess whether the response carried a partial-success message
    /// @param rejectedItems     the rejected item count
    /// @param errorMessage      the partial-success error message
    /// @param provenance        the provenance to attribute the result to
    /// @return the delivery result
    public static DeliveryResult deliveryResult(
            boolean hasPartialSuccess, long rejectedItems, String errorMessage,
            dev.nthings.otlp4j.core.result.Provenance provenance) {
        if (!hasPartialSuccess) {
            return DeliveryResult.accepted(provenance);
        }
        return DeliveryResult.partial(rejectedItems, errorMessage, provenance);
    }

    public static Attributes attributes(List<KeyValue> keyValues) {
        return attributes(keyValues, NO_STRINGS);
    }

    /// Maps attributes that may refer to a `ProfilesDictionary` string table through `strings`, which
    /// answers `null` for a reference it cannot resolve.
    static Attributes attributes(List<KeyValue> keyValues, IntFunction<@Nullable String> strings) {
        if (keyValues.isEmpty()) {
            return Attributes.empty();
        }
        var map = new LinkedHashMap<String, AttributeValue>(keyValues.size());
        for (var kv : keyValues) {
            map.put(key(kv, strings), attributeValue(kv.getValue(), strings));
        }
        return Attributes.of(map);
    }

    private static String key(KeyValue kv, IntFunction<@Nullable String> strings) {
        if (kv.getKeyStrindex() == 0) {
            return kv.getKey();
        }
        var key = strings.apply(kv.getKeyStrindex());
        return key == null ? kv.getKey() : key;
    }

    public static AttributeValue attributeValue(AnyValue value) {
        return attributeValue(value, NO_STRINGS);
    }

    /// Maps a value that may refer to a `ProfilesDictionary` string table through `strings`.
    static AttributeValue attributeValue(AnyValue value, IntFunction<@Nullable String> strings) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> AttributeValue.of(value.getStringValue());
            case BOOL_VALUE -> AttributeValue.of(value.getBoolValue());
            case INT_VALUE -> AttributeValue.of(value.getIntValue());
            case DOUBLE_VALUE -> AttributeValue.of(value.getDoubleValue());
            case BYTES_VALUE -> AttributeValue.of(value.getBytesValue().toByteArray());
            case ARRAY_VALUE -> {
                var values = new ArrayList<AttributeValue>(value.getArrayValue().getValuesCount());
                for (var element : value.getArrayValue().getValuesList()) {
                    values.add(attributeValue(element, strings));
                }
                yield AttributeValue.of(values);
            }
            case KVLIST_VALUE -> {
                var list = value.getKvlistValue();
                var map = new LinkedHashMap<String, AttributeValue>(list.getValuesCount());
                for (var kv : list.getValuesList()) {
                    map.put(key(kv, strings), attributeValue(kv.getValue(), strings));
                }
                yield AttributeValue.of(map);
            }
            case STRING_VALUE_STRINDEX -> {
                var string = strings.apply(value.getStringValueStrindex());
                yield string == null ? AttributeValue.empty() : AttributeValue.of(string);
            }
            case VALUE_NOT_SET -> AttributeValue.empty();
        };
    }

    public static Resource resource(io.opentelemetry.proto.resource.v1.Resource resource) {
        return resource(resource, NO_STRINGS);
    }

    static Resource resource(
            io.opentelemetry.proto.resource.v1.Resource resource, IntFunction<@Nullable String> strings) {
        return new Resource(
                attributes(resource.getAttributesList(), strings), resource.getDroppedAttributesCount());
    }

    public static InstrumentationScope scope(
            io.opentelemetry.proto.common.v1.InstrumentationScope scope) {
        return scope(scope, NO_STRINGS);
    }

    static InstrumentationScope scope(
            io.opentelemetry.proto.common.v1.InstrumentationScope scope, IntFunction<@Nullable String> strings) {
        return new InstrumentationScope(
                scope.getName(),
                scope.getVersion(),
                attributes(scope.getAttributesList(), strings),
                scope.getDroppedAttributesCount());
    }

    // --- domain -> proto ---

    /// Parses a lowercase-hex ID back into wire bytes. Empty string yields empty bytes.
    public static ByteString bytes(String hex) {
        return hex.isEmpty() ? ByteString.EMPTY : ByteString.copyFrom(HEX.parseHex(hex));
    }

    public static List<KeyValue> toKeyValues(Attributes attributes) {
        var result = new ArrayList<KeyValue>(attributes.size());
        attributes
                .asMap()
                .forEach((key, value) -> result.add(KeyValue.newBuilder()
                        .setKey(key)
                        .setValue(toAnyValue(value))
                        .build()));
        return result;
    }

    public static AnyValue toAnyValue(AttributeValue value) {
        return switch (value) {
            case AttributeValue.StringValue(var s) -> AnyValue.newBuilder().setStringValue(s).build();
            case AttributeValue.BoolValue(var b) -> AnyValue.newBuilder().setBoolValue(b).build();
            case AttributeValue.LongValue(var l) -> AnyValue.newBuilder().setIntValue(l).build();
            case AttributeValue.DoubleValue(var d) -> AnyValue.newBuilder().setDoubleValue(d).build();
            case AttributeValue.BytesValue(var bytes) ->
                AnyValue.newBuilder().setBytesValue(ByteString.copyFrom(bytes)).build();
            case AttributeValue.ArrayValue(var values) -> {
                var array = ArrayValue.newBuilder();
                for (var element : values) {
                    array.addValues(toAnyValue(element));
                }
                yield AnyValue.newBuilder().setArrayValue(array).build();
            }
            case AttributeValue.KeyValueListValue(var values) -> {
                var list = KeyValueList.newBuilder();
                values.forEach((key, element) -> list.addValues(KeyValue.newBuilder()
                        .setKey(key)
                        .setValue(toAnyValue(element))));
                yield AnyValue.newBuilder().setKvlistValue(list).build();
            }
            case AttributeValue.Empty() -> AnyValue.getDefaultInstance();
        };
    }

    public static io.opentelemetry.proto.resource.v1.Resource toProtoResource(Resource resource) {
        return toProtoResource(resource, toKeyValues(resource.attributes()));
    }

    /// Maps `resource` with its attributes already encoded as `attributes`.
    static io.opentelemetry.proto.resource.v1.Resource toProtoResource(Resource resource, List<KeyValue> attributes) {
        return io.opentelemetry.proto.resource.v1.Resource.newBuilder()
                .addAllAttributes(attributes)
                .setDroppedAttributesCount(resource.droppedAttributesCount())
                .build();
    }

    public static io.opentelemetry.proto.common.v1.InstrumentationScope toProtoScope(
            InstrumentationScope scope) {
        return toProtoScope(scope, toKeyValues(scope.attributes()));
    }

    /// Maps `scope` with its attributes already encoded as `attributes`.
    static io.opentelemetry.proto.common.v1.InstrumentationScope toProtoScope(
            InstrumentationScope scope, List<KeyValue> attributes) {
        return io.opentelemetry.proto.common.v1.InstrumentationScope.newBuilder()
                .setName(scope.name())
                .setVersion(scope.version())
                .addAllAttributes(attributes)
                .setDroppedAttributesCount(scope.droppedAttributesCount())
                .build();
    }
}
