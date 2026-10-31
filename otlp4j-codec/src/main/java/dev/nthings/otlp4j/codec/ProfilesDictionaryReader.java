package dev.nthings.otlp4j.codec;

import com.google.protobuf.ByteString;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.Profile;
import io.opentelemetry.proto.profiles.v1development.ProfilesDictionary;
import io.opentelemetry.proto.profiles.v1development.ValueType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;
import org.jspecify.annotations.Nullable;

/// Resolves the references of one profiles request against its `ProfilesDictionary`, so the domain
/// holds values rather than indices.
///
/// A reference outside its table fails with [IllegalArgumentException], because dropping it would
/// change what the profile says. The exception is index 0 of an absent table, which a request without
/// a dictionary uses to mean "not set" and which resolves to the zero value. Every other index 0 reads
/// the table's first entry like any index does: the proto requires that entry to be the zero value, and
/// a producer that broke the rule keeps its data.
///
/// Each entry resolves once, when first referenced, and is then shared, so decoding allocates in
/// proportion to the request however many references repeat an entry. A malformed entry nothing refers
/// to is harmless. Encoding does not share an attribute table value's strings, so those are bounded
/// here; see [#INLINE_EXPANSION].
final class ProfilesDictionaryReader {

    /// How many times the size of its dictionary a request's attribute table values may grow to by
    /// referring to the string table. Consumers read those values inline, so encoding writes each
    /// reference out in full, and without a limit one large string referenced many times would turn a
    /// small request into a huge one. No producer known to use such references is near the limit.
    private static final int INLINE_EXPANSION = 4;

    /// The growth every request may have, which keeps the ratio from refusing a small dictionary.
    private static final long INLINE_FLOOR_BYTES = 64 * 1024;

    /// The attributes an `attribute_indices` list resolves to, with the units of those that have one.
    record AttributeSet(Attributes attributes, Map<String, String> units) {
        private static final AttributeSet EMPTY = new AttributeSet(Attributes.empty(), Map.of());
    }

    /// One attribute table entry. The unit is empty when the attribute has none.
    private record Attribute(String key, AttributeValue value, String unit) {
        private static final Attribute NULL = new Attribute("", AttributeValue.empty(), "");
    }

    private final ProfilesDictionary dictionary;
    private final Profile.Stack[] stacks;
    private final Profile.Location[] locations;
    private final Profile.Mapping[] mappings;
    private final Profile.Function[] functions;
    private final Profile.Link[] links;
    private final Attribute[] attributes;
    // A sender chooses these keys, so a hash map could be made to degrade to a linear scan.
    private final Map<int[], AttributeSet> attributeSets = new TreeMap<>(Arrays::compare);
    private final long inlineLimit;
    private long inlined;

    ProfilesDictionaryReader(ProfilesDictionary dictionary) {
        this.dictionary = dictionary;
        this.stacks = new Profile.Stack[dictionary.getStackTableCount()];
        this.locations = new Profile.Location[dictionary.getLocationTableCount()];
        this.mappings = new Profile.Mapping[dictionary.getMappingTableCount()];
        this.functions = new Profile.Function[dictionary.getFunctionTableCount()];
        this.links = new Profile.Link[dictionary.getLinkTableCount()];
        this.attributes = new Attribute[dictionary.getAttributeTableCount()];
        this.inlineLimit = Math.max(INLINE_FLOOR_BYTES, (long) INLINE_EXPANSION * dictionary.getSerializedSize());
    }

    Profile.Stack stack(int index) {
        return resolve(stacks, index, "stack", Profile.Stack.EMPTY, this::readStack);
    }

    Profile.Link link(int index) {
        return resolve(links, index, "link", Profile.Link.NONE, this::readLink);
    }

    /// The string at `index`, which is also how attribute references to the string table resolve.
    String string(int index) {
        var string = stringOrNull(dictionary, index);
        if (string == null) {
            throw outside(index, dictionary.getStringTableCount(), "string");
        }
        return string;
    }

    /// The string at `index` of `dictionary`, or `null` outside its string table.
    static @Nullable String stringOrNull(ProfilesDictionary dictionary, int index) {
        if (index == 0 && dictionary.getStringTableCount() == 0) {
            return "";
        }
        return index >= 0 && index < dictionary.getStringTableCount() ? dictionary.getStringTable(index) : null;
    }

    Profile.ValueType valueType(ValueType valueType) {
        return new Profile.ValueType(string(valueType.getTypeStrindex()), string(valueType.getUnitStrindex()));
    }

    /// The attributes `indices` refer to. Repeated lists share one result, and references to the zero
    /// entry are skipped. Keys are unique on a valid request; the last of several duplicates wins.
    AttributeSet attributes(List<Integer> indices) {
        if (indices.isEmpty()) {
            return AttributeSet.EMPTY;
        }
        var key = new int[indices.size()];
        for (var i = 0; i < key.length; i++) {
            key[i] = indices.get(i);
        }
        return attributeSets.computeIfAbsent(key, this::readAttributes);
    }

    private AttributeSet readAttributes(int[] indices) {
        var values = new LinkedHashMap<String, AttributeValue>(indices.length);
        var units = new HashMap<String, String>();
        for (var index : indices) {
            var attribute = attribute(index);
            if (attribute != Attribute.NULL) {
                values.put(attribute.key(), attribute.value());
                units.put(attribute.key(), attribute.unit());
            }
        }
        units.values().removeIf(String::isEmpty);
        return new AttributeSet(Attributes.of(values), Profile.immutableUnits(units));
    }

    private Attribute attribute(int index) {
        return resolve(attributes, index, "attribute", Attribute.NULL, this::readAttribute);
    }

    private Attribute readAttribute(int index) {
        var entry = dictionary.getAttributeTable(index);
        if (entry.getSerializedSize() == 0) {
            return Attribute.NULL;
        }
        return new Attribute(
                string(entry.getKeyStrindex()),
                CommonMapper.attributeValue(entry.getValue(), this::inlinedString),
                string(entry.getUnitStrindex()));
    }

    private String inlinedString(int index) {
        var string = string(index);
        if (!string.isEmpty()) {
            inlined += dictionary.getStringTableBytes(index).size();
        }
        if (inlined > inlineLimit) {
            throw new IllegalArgumentException(
                    "profile attribute values expand past " + inlineLimit + " bytes of inlined strings");
        }
        return string;
    }

    private Profile.Stack readStack(int index) {
        var stack = dictionary.getStackTable(index);
        var frames = new ArrayList<Profile.Location>(stack.getLocationIndicesCount());
        for (int location : stack.getLocationIndicesList()) {
            frames.add(location(location));
        }
        return new Profile.Stack(frames);
    }

    private Profile.Location location(int index) {
        return resolve(locations, index, "location", Profile.Location.EMPTY, this::readLocation);
    }

    private Profile.Location readLocation(int index) {
        var location = dictionary.getLocationTable(index);
        var lines = new ArrayList<Profile.Line>(location.getLinesCount());
        for (var line : location.getLinesList()) {
            lines.add(new Profile.Line(function(line.getFunctionIndex()), line.getLine(), line.getColumn()));
        }
        var attributes = attributes(location.getAttributeIndicesList());
        return new Profile.Location(
                mapping(location.getMappingIndex()),
                location.getAddress(),
                lines,
                attributes.attributes(),
                attributes.units());
    }

    private Profile.Mapping mapping(int index) {
        return resolve(mappings, index, "mapping", Profile.Mapping.EMPTY, this::readMapping);
    }

    private Profile.Mapping readMapping(int index) {
        var mapping = dictionary.getMappingTable(index);
        var attributes = attributes(mapping.getAttributeIndicesList());
        return new Profile.Mapping(
                mapping.getMemoryStart(),
                mapping.getMemoryLimit(),
                mapping.getFileOffset(),
                string(mapping.getFilenameStrindex()),
                attributes.attributes(),
                attributes.units());
    }

    private Profile.Function function(int index) {
        return resolve(functions, index, "function", Profile.Function.EMPTY, this::readFunction);
    }

    private Profile.Function readFunction(int index) {
        var function = dictionary.getFunctionTable(index);
        return new Profile.Function(
                string(function.getNameStrindex()),
                string(function.getSystemNameStrindex()),
                string(function.getFilenameStrindex()),
                function.getStartLine());
    }

    private Profile.Link readLink(int index) {
        var link = dictionary.getLinkTable(index);
        var traceId = link.getTraceId();
        var spanId = link.getSpanId();
        // The proto recommends all-zero ids for link_table[0], so another entry can spell "no link" too.
        return isZero(traceId) && isZero(spanId)
                ? Profile.Link.NONE
                : new Profile.Link(CommonMapper.hex(traceId), CommonMapper.hex(spanId));
    }

    private static boolean isZero(ByteString id) {
        for (var i = 0; i < id.size(); i++) {
            if (id.byteAt(i) != 0) {
                return false;
            }
        }
        return true;
    }

    private static <V> V resolve(V[] table, int index, String name, V zero, IntFunction<V> read) {
        if (index == 0 && table.length == 0) {
            return zero;
        }
        if (index < 0 || index >= table.length) {
            throw outside(index, table.length, name);
        }
        var value = table[index];
        if (value == null) {
            value = read.apply(index);
            table[index] = value;
        }
        return value;
    }

    private static IllegalArgumentException outside(int index, int size, String name) {
        return new IllegalArgumentException(
                "profile " + name + " reference " + index + " is outside a table of " + size + " entries");
    }
}
