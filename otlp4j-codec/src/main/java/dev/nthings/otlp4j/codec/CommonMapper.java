package dev.nthings.otlp4j.codec;

import com.google.protobuf.ByteString;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.ConsumeResult;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Maps OTLP common/resource types between the generated proto layer and the pure domain model,
/// in both directions.
///
/// **Internal.** Part of the transport layer; not public API. The generated proto
/// classes are confined to this package and never escape into the domain or high-level layers.
final class CommonMapper {

    private static final HexFormat HEX = HexFormat.of();

    private CommonMapper() {}

    // --- proto -> domain ---------------------------------------------------------------------

    /// Renders an id (trace id, span id, profile id) as a lowercase-hex string.
    public static String hex(ByteString bytes) {
        return bytes.isEmpty() ? "" : HEX.formatHex(bytes.toByteArray());
    }

    /// Interprets an OTLP export response's partial-success block as a [ConsumeResult]. Shared by
    /// all four signal mappers, which differ only in the `rejected_*` accessor they read.
    ///
    /// A zero rejected count with a non-empty message is a whole-batch [ConsumeResult.Rejected], not a
    /// partial success; a positive count is a [ConsumeResult.Partial]; anything else is accepted.
    public static <T> ConsumeResult<T> result(
            boolean hasPartialSuccess, long rejectedItems, String errorMessage) {
        if (!hasPartialSuccess || (rejectedItems == 0 && errorMessage.isEmpty())) {
            return ConsumeResult.accepted();
        }
        if (rejectedItems == 0) {
            return ConsumeResult.rejected(errorMessage);
        }
        return ConsumeResult.partial(rejectedItems, errorMessage);
    }

    public static Attributes attributes(List<KeyValue> keyValues) {
        if (keyValues.isEmpty()) {
            return Attributes.empty();
        }
        var map = new LinkedHashMap<String, AttributeValue>(keyValues.size());
        for (var kv : keyValues) {
            map.put(kv.getKey(), attributeValue(kv.getValue()));
        }
        return Attributes.of(map);
    }

    public static AttributeValue attributeValue(AnyValue value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> AttributeValue.of(value.getStringValue());
            case BOOL_VALUE -> AttributeValue.of(value.getBoolValue());
            case INT_VALUE -> AttributeValue.of(value.getIntValue());
            case DOUBLE_VALUE -> AttributeValue.of(value.getDoubleValue());
            case BYTES_VALUE -> AttributeValue.of(value.getBytesValue().toByteArray());
            case ARRAY_VALUE -> {
                var values = new ArrayList<AttributeValue>(value.getArrayValue().getValuesCount());
                for (var element : value.getArrayValue().getValuesList()) {
                    values.add(attributeValue(element));
                }
                yield AttributeValue.of(values);
            }
            case KVLIST_VALUE -> {
                var list = value.getKvlistValue();
                var map = new LinkedHashMap<String, AttributeValue>(list.getValuesCount());
                for (var kv : list.getValuesList()) {
                    map.put(kv.getKey(), attributeValue(kv.getValue()));
                }
                yield AttributeValue.of(map);
            }
            // string_value_strindex is a profiling-only reference into ProfilesDictionary.
            // Forwarded profiles round-trip losslessly via opaque payload passthrough, but the
            // modeled attribute view still surfaces this as empty because the dictionary is not
            // resolved into modeled attributes.
            case STRING_VALUE_STRINDEX, VALUE_NOT_SET -> AttributeValue.empty();
        };
    }

    public static Resource resource(io.opentelemetry.proto.resource.v1.Resource resource) {
        return new Resource(
                attributes(resource.getAttributesList()), resource.getDroppedAttributesCount());
    }

    public static InstrumentationScope scope(
            io.opentelemetry.proto.common.v1.InstrumentationScope scope) {
        return new InstrumentationScope(
                scope.getName(),
                scope.getVersion(),
                attributes(scope.getAttributesList()),
                scope.getDroppedAttributesCount());
    }

    // --- domain -> proto ---------------------------------------------------------------------

    /// Parses a lowercase-hex id back into its wire bytes; an empty string yields empty bytes.
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
        return io.opentelemetry.proto.resource.v1.Resource.newBuilder()
                .addAllAttributes(toKeyValues(resource.attributes()))
                .setDroppedAttributesCount(resource.droppedAttributesCount())
                .build();
    }

    public static io.opentelemetry.proto.common.v1.InstrumentationScope toProtoScope(
            InstrumentationScope scope) {
        return io.opentelemetry.proto.common.v1.InstrumentationScope.newBuilder()
                .setName(scope.name())
                .setVersion(scope.version())
                .addAllAttributes(toKeyValues(scope.attributes()))
                .setDroppedAttributesCount(scope.droppedAttributesCount())
                .build();
    }
}
