package dev.nthings.otlp4j.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.Profile;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.profiles.v1development.Function;
import io.opentelemetry.proto.profiles.v1development.KeyValueAndUnit;
import io.opentelemetry.proto.profiles.v1development.Line;
import io.opentelemetry.proto.profiles.v1development.Link;
import io.opentelemetry.proto.profiles.v1development.Location;
import io.opentelemetry.proto.profiles.v1development.Mapping;
import io.opentelemetry.proto.profiles.v1development.ProfilesDictionary;
import io.opentelemetry.proto.profiles.v1development.Stack;
import io.opentelemetry.proto.profiles.v1development.ValueType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntConsumer;

/// Builds the `ProfilesDictionary` of one encoded batch from the values its profiles hold.
///
/// Tables hold exactly what the batch refers to, once each, in first-use order, so a batch encodes to
/// the same bytes every time. Every table starts with the zero value the proto requires, and a value
/// that encodes like that entry, such as [Profile.Link#NONE], is a reference to it.
///
/// Telemetry that came from a sender decides what is deduplicated here, so nothing may degrade with
/// its content. Values are never hashed: an entry refers to its children by index, which bounds its
/// encoded form, and that form is compared in a tree.
final class ProfilesDictionaryWriter {

    /// The proto recommends this over an empty link for codecs that insist on id lengths.
    private static final Link ZERO_LINK = Link.newBuilder()
            .setTraceId(ByteString.copyFrom(new byte[16]))
            .setSpanId(ByteString.copyFrom(new byte[8]))
            .build();

    private final ProfilesDictionary.Builder dictionary = ProfilesDictionary.newBuilder();
    private final Map<String, Integer> strings = new HashMap<>();
    private final Table<Profile.Stack, Stack> stacks =
            new Table<>(Stack.getDefaultInstance(), this::stackEntry);
    private final Table<Profile.Location, Location> locations =
            new Table<>(Location.getDefaultInstance(), this::locationEntry);
    private final Table<Profile.Mapping, Mapping> mappings =
            new Table<>(Mapping.getDefaultInstance(), this::mappingEntry);
    private final Table<Profile.Function, Function> functions =
            new Table<>(Function.getDefaultInstance(), this::functionEntry);
    private final Table<Profile.Link, Link> links = new Table<>(ZERO_LINK, ProfilesDictionaryWriter::linkEntry);
    /// Attribute values by the id an attribute table entry is keyed on; never a dictionary table.
    private final Table<AttributeValue, AnyValue> attributeValues =
            new Table<>(AnyValue.getDefaultInstance(), CommonMapper::toAnyValue);
    private final Map<int[], Integer> attributes = new TreeMap<>(Arrays::compare);

    ProfilesDictionaryWriter() {
        strings.put("", 0);
        dictionary.addStringTable("");
        dictionary.addAttributeTable(KeyValueAndUnit.getDefaultInstance());
    }

    int stack(Profile.Stack stack) {
        return stacks.index(stack);
    }

    int link(Profile.Link link) {
        return links.index(link);
    }

    ValueType valueType(Profile.ValueType valueType) {
        return ValueType.newBuilder()
                .setTypeStrindex(string(valueType.type()))
                .setUnitStrindex(string(valueType.unit()))
                .build();
    }

    /// Hands `add` the attribute table index of each of `values`, in order.
    void attributes(Attributes values, Map<String, String> units, IntConsumer add) {
        values.asMap().forEach((key, value) -> add.accept(attribute(key, value, units.getOrDefault(key, ""))));
    }

    /// Encodes the attributes of a resource or scope with every key and string held in the dictionary,
    /// which is how the Collector writes and reads them. A string a request repeats by reference
    /// therefore stays one string, however many resources repeat it.
    List<KeyValue> references(Attributes values) {
        var keyValues = new ArrayList<KeyValue>(values.size());
        values.asMap().forEach((key, value) -> keyValues.add(reference(key, value)));
        return keyValues;
    }

    private KeyValue reference(String key, AttributeValue value) {
        return KeyValue.newBuilder().setKeyStrindex(string(key)).setValue(reference(value)).build();
    }

    private AnyValue reference(AttributeValue value) {
        return switch (value) {
            case AttributeValue.StringValue(var string) ->
                AnyValue.newBuilder().setStringValueStrindex(string(string)).build();
            case AttributeValue.ArrayValue(var values) -> {
                var array = ArrayValue.newBuilder();
                values.forEach(element -> array.addValues(reference(element)));
                yield AnyValue.newBuilder().setArrayValue(array).build();
            }
            case AttributeValue.KeyValueListValue(var values) -> {
                var list = KeyValueList.newBuilder();
                values.forEach((key, element) -> list.addValues(reference(key, element)));
                yield AnyValue.newBuilder().setKvlistValue(list).build();
            }
            default -> CommonMapper.toAnyValue(value);
        };
    }

    ProfilesDictionary build() {
        return dictionary
                .addAllStackTable(stacks.entries)
                .addAllLocationTable(locations.entries)
                .addAllMappingTable(mappings.entries)
                .addAllFunctionTable(functions.entries)
                .addAllLinkTable(links.entries)
                .build();
    }

    private int attribute(String key, AttributeValue value, String unit) {
        var keyIndex = string(key);
        var unitIndex = string(unit);
        var valueId = attributeValues.index(value);
        return attributes.computeIfAbsent(new int[] {keyIndex, valueId, unitIndex}, ignored -> {
            dictionary.addAttributeTable(KeyValueAndUnit.newBuilder()
                    .setKeyStrindex(keyIndex)
                    .setValue(attributeValues.entries.get(valueId))
                    .setUnitStrindex(unitIndex));
            return dictionary.getAttributeTableCount() - 1;
        });
    }

    private int string(String value) {
        var index = strings.get(value);
        if (index == null) {
            index = dictionary.getStringTableCount();
            strings.put(value, index);
            dictionary.addStringTable(value);
        }
        return index;
    }

    private Stack stackEntry(Profile.Stack stack) {
        var entry = Stack.newBuilder();
        for (var location : stack.locations()) {
            entry.addLocationIndices(locations.index(location));
        }
        return entry.build();
    }

    private Location locationEntry(Profile.Location location) {
        var entry = Location.newBuilder()
                .setMappingIndex(mappings.index(location.mapping()))
                .setAddress(location.address());
        for (var line : location.lines()) {
            entry.addLines(Line.newBuilder()
                    .setFunctionIndex(functions.index(line.function()))
                    .setLine(line.line())
                    .setColumn(line.column()));
        }
        attributes(location.attributes(), location.attributeUnits(), entry::addAttributeIndices);
        return entry.build();
    }

    private Mapping mappingEntry(Profile.Mapping mapping) {
        var entry = Mapping.newBuilder()
                .setMemoryStart(mapping.memoryStart())
                .setMemoryLimit(mapping.memoryLimit())
                .setFileOffset(mapping.fileOffset())
                .setFilenameStrindex(string(mapping.filename()));
        attributes(mapping.attributes(), mapping.attributeUnits(), entry::addAttributeIndices);
        return entry.build();
    }

    private Function functionEntry(Profile.Function function) {
        return Function.newBuilder()
                .setNameStrindex(string(function.name()))
                .setSystemNameStrindex(string(function.systemName()))
                .setFilenameStrindex(string(function.filename()))
                .setStartLine(function.startLine())
                .build();
    }

    private static Link linkEntry(Profile.Link link) {
        return link.equals(Profile.Link.NONE) ? ZERO_LINK : Link.newBuilder()
                .setTraceId(CommonMapper.bytes(link.traceId()))
                .setSpanId(CommonMapper.bytes(link.spanId()))
                .build();
    }

    /// One table's entries, each unique, starting with its zero value. Identity answers first, because
    /// decoding shares an entry across every sample that repeats it and encoding that entry again would
    /// serve no purpose.
    private static final class Table<V, E extends MessageLite> {

        private final List<E> entries = new ArrayList<>();
        private final Map<V, Integer> byIdentity = new IdentityHashMap<>();
        private final Map<byte[], Integer> byEntry = new TreeMap<>(Arrays::compareUnsigned);
        private final java.util.function.Function<V, E> entry;

        private Table(E zero, java.util.function.Function<V, E> entry) {
            this.entry = entry;
            entries.add(zero);
            byEntry.put(zero.toByteArray(), 0);
        }

        private int index(V value) {
            return byIdentity.computeIfAbsent(value, unseen -> {
                var built = entry.apply(unseen);
                return byEntry.computeIfAbsent(built.toByteArray(), encoded -> {
                    entries.add(built);
                    return entries.size() - 1;
                });
            });
        }
    }
}
