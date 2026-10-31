package dev.nthings.otlp4j.opentelemetry;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.Profile;
import dev.nthings.otlp4j.model.ProfilesData;
import io.opentelemetry.sdk.profiles.data.ProfileData;
import io.opentelemetry.sdk.profiles.data.ProfilesDictionaryData;
import io.opentelemetry.sdk.profiles.data.SampleData;
import io.opentelemetry.sdk.profiles.data.ValueTypeData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

/// Resolves SDK profiles against one `ProfilesDictionaryData`, sharing each entry it has resolved so a
/// stack repeated by many samples stays one object. Index 0 is every table's null reference and needs
/// no table, and an index outside its table fails the export, as it does for a malformed OTLP request.
final class SdkProfileResolver {

    /// Resolved attributes with the units of those that have one.
    private record AttributeSet(Attributes attributes, Map<String, String> units) {}

    private final ProfilesDictionaryData dictionary;
    private final Map<Integer, Profile.Stack> stacks = new HashMap<>();
    private final Map<Integer, Profile.Location> locations = new HashMap<>();
    private final Map<Integer, Profile.Mapping> mappings = new HashMap<>();
    private final Map<Integer, Profile.Function> functions = new HashMap<>();
    private final Map<Integer, Profile.Link> links = new HashMap<>();
    private final Map<List<Integer>, AttributeSet> attributeSets = new HashMap<>();

    private SdkProfileResolver(ProfilesDictionaryData dictionary) {
        this.dictionary = dictionary;
    }

    /// Maps SDK profiles to one batch, grouped by resource and scope. The SDK attaches a dictionary to
    /// each profile rather than to the batch, and sharing a resolver keeps an entry repeated by the
    /// profiles of one dictionary a single object.
    static ProfilesData batch(Collection<ProfileData> profiles) {
        var resolvers = new IdentityHashMap<ProfilesDictionaryData, SdkProfileResolver>();
        return new ProfilesData(SdkDataMapper.group(
                profiles,
                ProfileData::getResource,
                ProfileData::getInstrumentationScopeInfo,
                profile -> resolvers.computeIfAbsent(profile.getProfileDictionaryData(), SdkProfileResolver::new)
                        .profile(profile),
                ProfilesData.ScopeProfiles::new,
                ProfilesData.ResourceProfiles::new));
    }

    private Profile profile(ProfileData profile) {
        var samples = new ArrayList<Profile.Sample>(profile.getSamples().size());
        for (var sample : profile.getSamples()) {
            samples.add(sample(sample));
        }
        var attributes = attributes(profile.getAttributeIndices());
        return new Profile(
                profile.getProfileId(),
                profile.getTimeNanos(),
                profile.getDurationNanos(),
                valueType(profile.getSampleType()),
                valueType(profile.getPeriodType()),
                profile.getPeriod(),
                samples,
                attributes.attributes(),
                attributes.units(),
                SdkDataMapper.dropped(profile.getTotalAttributeCount(), profile.getAttributeIndices().size()),
                SdkDataMapper.nullToEmpty(profile.getOriginalPayloadFormat()),
                profile.getOriginalPayload() == null ? new byte[0] : SdkDataMapper.bytes(profile.getOriginalPayload()));
    }

    private Profile.Sample sample(SampleData sample) {
        var attributes = attributes(sample.getAttributeIndices());
        return new Profile.Sample(
                stack(sample.getStackIndex()),
                attributes.attributes(),
                attributes.units(),
                link(sample.getLinkIndex()),
                sample.getValues(),
                sample.getTimestamps());
    }

    private Profile.ValueType valueType(ValueTypeData valueType) {
        return new Profile.ValueType(
                string(valueType.getTypeStringIndex()), string(valueType.getUnitStringIndex()));
    }

    private Profile.Stack stack(int index) {
        return resolve(stacks, index, Profile.Stack.EMPTY, this::readStack);
    }

    private Profile.Stack readStack(int index) {
        var frames = new ArrayList<Profile.Location>();
        for (int location : dictionary.getStackTable().get(index).getLocationIndices()) {
            frames.add(location(location));
        }
        return new Profile.Stack(frames);
    }

    private Profile.Location location(int index) {
        return resolve(locations, index, Profile.Location.EMPTY, this::readLocation);
    }

    private Profile.Location readLocation(int index) {
        var location = dictionary.getLocationTable().get(index);
        var lines = new ArrayList<Profile.Line>(location.getLines().size());
        for (var line : location.getLines()) {
            lines.add(new Profile.Line(function(line.getFunctionIndex()), line.getLine(), line.getColumn()));
        }
        var attributes = attributes(location.getAttributeIndices());
        return new Profile.Location(
                mapping(location.getMappingIndex()),
                location.getAddress(),
                lines,
                attributes.attributes(),
                attributes.units());
    }

    private Profile.Mapping mapping(int index) {
        return resolve(mappings, index, Profile.Mapping.EMPTY, this::readMapping);
    }

    private Profile.Mapping readMapping(int index) {
        var mapping = dictionary.getMappingTable().get(index);
        var attributes = attributes(mapping.getAttributeIndices());
        return new Profile.Mapping(
                mapping.getMemoryStart(),
                mapping.getMemoryLimit(),
                mapping.getFileOffset(),
                string(mapping.getFilenameStringIndex()),
                attributes.attributes(),
                attributes.units());
    }

    private Profile.Function function(int index) {
        return resolve(functions, index, Profile.Function.EMPTY, this::readFunction);
    }

    private Profile.Function readFunction(int index) {
        var function = dictionary.getFunctionTable().get(index);
        return new Profile.Function(
                string(function.getNameStringIndex()),
                string(function.getSystemNameStringIndex()),
                string(function.getFilenameStringIndex()),
                function.getStartLine());
    }

    private Profile.Link link(int index) {
        return resolve(links, index, Profile.Link.NONE, this::readLink);
    }

    private Profile.Link readLink(int index) {
        var link = dictionary.getLinkTable().get(index);
        return new Profile.Link(link.getTraceId(), link.getSpanId());
    }

    private String string(int index) {
        return index == 0 ? "" : dictionary.getStringTable().get(index);
    }

    private AttributeSet attributes(List<Integer> indices) {
        return attributeSets.computeIfAbsent(indices, this::readAttributes);
    }

    private AttributeSet readAttributes(List<Integer> indices) {
        var values = new LinkedHashMap<String, AttributeValue>(indices.size());
        var units = new HashMap<String, String>();
        for (int index : indices) {
            if (index != 0) {
                var entry = dictionary.getAttributeTable().get(index);
                var key = string(entry.getKeyStringIndex());
                values.put(key, SdkDataMapper.attributeValueOf(entry.getValue()));
                units.put(key, string(entry.getUnitStringIndex()));
            }
        }
        units.values().removeIf(String::isEmpty);
        return new AttributeSet(Attributes.of(values), Profile.immutableUnits(units));
    }

    private static <V> V resolve(Map<Integer, V> resolved, int index, V zero, IntFunction<V> read) {
        return index == 0 ? zero : resolved.computeIfAbsent(index, read::apply);
    }
}
