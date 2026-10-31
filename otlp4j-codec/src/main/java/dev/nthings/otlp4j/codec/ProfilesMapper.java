package dev.nthings.otlp4j.codec;

import com.google.protobuf.UnsafeByteOperations;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.Profile;
import dev.nthings.otlp4j.model.ProfilesData;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.profiles.v1development.ResourceProfiles;
import io.opentelemetry.proto.profiles.v1development.Sample;
import io.opentelemetry.proto.profiles.v1development.ScopeProfiles;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import org.jspecify.annotations.Nullable;

/// Maps profiles between proto and [ProfilesData].
///
/// Profiles use the `v1development` spec. Decoding resolves every reference into the request's
/// `ProfilesDictionary`, and encoding builds a new dictionary holding only what the batch refers to,
/// so the wire form of a decoded batch keeps its meaning without keeping its indices. A `Payload` that
/// is forwarded without a change never comes here and keeps its original request.
final class ProfilesMapper {

    private ProfilesMapper() {}

    // --- proto item count ---

    /// Counts profiles in a proto profiles export request without decoding it to the domain, walking
    /// the full resource then scope then profile hierarchy so the count agrees with
    /// `ProfilesData.profileCount`.
    ///
    /// @param request the proto profiles export request
    /// @return the total profile count
    static long itemCount(ExportProfilesServiceRequest request) {
        long count = 0;
        for (var rp : request.getResourceProfilesList()) {
            for (var sp : rp.getScopeProfilesList()) {
                count += sp.getProfilesCount();
            }
        }
        return count;
    }

    /// The resource-level attributes of a proto profiles export request, one entry per resource in wire
    /// order. Converts only the top-level resource attribute lists through [CommonMapper#attributes],
    /// so the profiles are never decoded and no domain batch is materialised. Attributes may refer to
    /// the dictionary's string table; a reference outside it reads as absent, because a payload that
    /// only passes through must not fail on what forwarding never interprets.
    ///
    /// @param request the proto profiles export request
    /// @return the per-resource attributes in wire order
    static List<Attributes> protoResourceAttributes(ExportProfilesServiceRequest request) {
        var dictionary = request.getDictionary();
        IntFunction<@Nullable String> strings = index -> ProfilesDictionaryReader.stringOrNull(dictionary, index);
        var result = new ArrayList<Attributes>(request.getResourceProfilesCount());
        for (var rp : request.getResourceProfilesList()) {
            result.add(CommonMapper.attributes(rp.getResource().getAttributesList(), strings));
        }
        return result;
    }

    // --- proto -> domain ---

    static ProfilesData toDomain(ExportProfilesServiceRequest request) {
        var dictionary = new ProfilesDictionaryReader(request.getDictionary());
        var resourceProfiles =
                new ArrayList<ProfilesData.ResourceProfiles>(request.getResourceProfilesCount());
        for (var rp : request.getResourceProfilesList()) {
            resourceProfiles.add(toDomain(rp, dictionary));
        }
        return new ProfilesData(resourceProfiles);
    }

    private static ProfilesData.ResourceProfiles toDomain(
            ResourceProfiles rp, ProfilesDictionaryReader dictionary) {
        var scopeProfiles =
                new ArrayList<ProfilesData.ScopeProfiles>(rp.getScopeProfilesCount());
        for (var sp : rp.getScopeProfilesList()) {
            scopeProfiles.add(toDomain(sp, dictionary));
        }
        return new ProfilesData.ResourceProfiles(
                CommonMapper.resource(rp.getResource(), dictionary::string), rp.getSchemaUrl(), scopeProfiles);
    }

    private static ProfilesData.ScopeProfiles toDomain(
            ScopeProfiles sp, ProfilesDictionaryReader dictionary) {
        var profiles = new ArrayList<Profile>(sp.getProfilesCount());
        for (var profile : sp.getProfilesList()) {
            profiles.add(toDomain(profile, dictionary));
        }
        return new ProfilesData.ScopeProfiles(
                CommonMapper.scope(sp.getScope(), dictionary::string), sp.getSchemaUrl(), profiles);
    }

    private static Profile toDomain(
            io.opentelemetry.proto.profiles.v1development.Profile profile, ProfilesDictionaryReader dictionary) {
        var samples = new ArrayList<Profile.Sample>(profile.getSamplesCount());
        for (var sample : profile.getSamplesList()) {
            samples.add(toDomain(sample, dictionary));
        }
        var attributes = dictionary.attributes(profile.getAttributeIndicesList());
        return new Profile(
                CommonMapper.hex(profile.getProfileId()),
                profile.getTimeUnixNano(),
                profile.getDurationNano(),
                dictionary.valueType(profile.getSampleType()),
                dictionary.valueType(profile.getPeriodType()),
                profile.getPeriod(),
                samples,
                attributes.attributes(),
                attributes.units(),
                profile.getDroppedAttributesCount(),
                profile.getOriginalPayloadFormat(),
                profile.getOriginalPayload().toByteArray());
    }

    private static Profile.Sample toDomain(Sample sample, ProfilesDictionaryReader dictionary) {
        var attributes = dictionary.attributes(sample.getAttributeIndicesList());
        return new Profile.Sample(
                dictionary.stack(sample.getStackIndex()),
                attributes.attributes(),
                attributes.units(),
                dictionary.link(sample.getLinkIndex()),
                sample.getValuesList(),
                sample.getTimestampsUnixNanoList());
    }

    /// Interprets an OTLP profiles export response as a
    /// [dev.nthings.otlp4j.core.result.DeliveryResult].
    ///
    /// @param response   the profiles export response
    /// @param provenance the provenance to attribute the result to
    /// @return the delivery result
    static dev.nthings.otlp4j.core.result.DeliveryResult deliveryResult(
            ExportProfilesServiceResponse response, dev.nthings.otlp4j.core.result.Provenance provenance) {
        var partial = response.getPartialSuccess();
        return CommonMapper.deliveryResult(
                response.hasPartialSuccess(), partial.getRejectedProfiles(), partial.getErrorMessage(), provenance);
    }

    // --- domain -> proto ---

    static ExportProfilesServiceRequest toProto(ProfilesData profiles) {
        var dictionary = new ProfilesDictionaryWriter();
        var request = ExportProfilesServiceRequest.newBuilder();
        for (var rp : profiles.resourceProfiles()) {
            request.addResourceProfiles(toProto(rp, dictionary));
        }
        if (!profiles.resourceProfiles().isEmpty()) {
            request.setDictionary(dictionary.build());
        }
        return request.build();
    }

    private static ResourceProfiles toProto(
            ProfilesData.ResourceProfiles rp, ProfilesDictionaryWriter dictionary) {
        var builder =
                ResourceProfiles.newBuilder()
                        .setResource(CommonMapper.toProtoResource(
                                rp.resource(), dictionary.references(rp.resource().attributes())))
                        .setSchemaUrl(rp.schemaUrl());
        for (var sp : rp.scopeProfiles()) {
            builder.addScopeProfiles(toProto(sp, dictionary));
        }
        return builder.build();
    }

    private static ScopeProfiles toProto(
            ProfilesData.ScopeProfiles sp, ProfilesDictionaryWriter dictionary) {
        var builder =
                ScopeProfiles.newBuilder()
                        .setScope(CommonMapper.toProtoScope(
                                sp.scope(), dictionary.references(sp.scope().attributes())))
                        .setSchemaUrl(sp.schemaUrl());
        for (var profile : sp.profiles()) {
            builder.addProfiles(toProto(profile, dictionary));
        }
        return builder.build();
    }

    private static io.opentelemetry.proto.profiles.v1development.Profile toProto(
            Profile profile, ProfilesDictionaryWriter dictionary) {
        var builder = io.opentelemetry.proto.profiles.v1development.Profile.newBuilder()
                .setProfileId(CommonMapper.bytes(profile.profileId()))
                .setTimeUnixNano(profile.epochNanos())
                .setDurationNano(profile.durationNanos())
                .setPeriod(profile.period())
                .setDroppedAttributesCount(profile.droppedAttributesCount())
                .setOriginalPayloadFormat(profile.originalPayloadFormat())
                // The accessor returns a private copy, so wrapping it saves a second one.
                .setOriginalPayload(UnsafeByteOperations.unsafeWrap(profile.originalPayload()));
        if (!Profile.ValueType.EMPTY.equals(profile.sampleType())) {
            builder.setSampleType(dictionary.valueType(profile.sampleType()));
        }
        if (!Profile.ValueType.EMPTY.equals(profile.periodType())) {
            builder.setPeriodType(dictionary.valueType(profile.periodType()));
        }
        for (var sample : profile.samples()) {
            builder.addSamples(toProto(sample, dictionary));
        }
        dictionary.attributes(profile.attributes(), profile.attributeUnits(), builder::addAttributeIndices);
        return builder.build();
    }

    private static Sample toProto(Profile.Sample sample, ProfilesDictionaryWriter dictionary) {
        var builder = Sample.newBuilder()
                .setStackIndex(dictionary.stack(sample.stack()))
                .setLinkIndex(dictionary.link(sample.link()))
                .addAllValues(sample.values())
                .addAllTimestampsUnixNano(sample.timestamps());
        dictionary.attributes(sample.attributes(), sample.attributeUnits(), builder::addAttributeIndices);
        return builder.build();
    }
}
