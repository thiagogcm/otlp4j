package dev.nthings.otlp4j.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.profiles.v1development.ProfilesDictionary;
import io.opentelemetry.proto.profiles.v1development.Profile;
import io.opentelemetry.proto.profiles.v1development.ResourceProfiles;
import io.opentelemetry.proto.profiles.v1development.ScopeProfiles;
import java.util.ArrayList;

/// Maps profiles between proto and [ProfilesData].
///
/// Profiles use the `v1development` spec. Only top-level metadata is modeled for inspection;
/// forwarding is lossless via opaque payload passthrough so domain-to-proto re-emits
/// byte-identical output.
public final class ProfilesMapper {

    private ProfilesMapper() {}

    // --- proto -> domain ---

    public static ProfilesData toDomain(ExportProfilesServiceRequest request) {
        var resourceProfiles =
                new ArrayList<ProfilesData.ResourceProfiles>(request.getResourceProfilesCount());
        for (var rp : request.getResourceProfilesList()) {
            resourceProfiles.add(toDomain(rp));
        }
        var dictionary =
                request.hasDictionary() ? request.getDictionary().toByteArray() : new byte[0];
        return new ProfilesData(resourceProfiles, dictionary);
    }

    private static ProfilesData.ResourceProfiles toDomain(
            ResourceProfiles rp) {
        var scopeProfiles =
                new ArrayList<ProfilesData.ScopeProfiles>(rp.getScopeProfilesCount());
        for (var sp : rp.getScopeProfilesList()) {
            scopeProfiles.add(toDomain(sp));
        }
        return new ProfilesData.ResourceProfiles(
                CommonMapper.resource(rp.getResource()), rp.getSchemaUrl(), scopeProfiles);
    }

    private static ProfilesData.ScopeProfiles toDomain(
            ScopeProfiles sp) {
        var profiles = new ArrayList<ProfilesData.Profile>(sp.getProfilesCount());
        for (var profile : sp.getProfilesList()) {
            profiles.add(toDomain(profile));
        }
        return new ProfilesData.ScopeProfiles(
                CommonMapper.scope(sp.getScope()), sp.getSchemaUrl(), profiles);
    }

    private static ProfilesData.Profile toDomain(
            Profile profile) {
        return new ProfilesData.Profile(
                CommonMapper.hex(profile.getProfileId()),
                profile.getTimeUnixNano(),
                profile.getDurationNano(),
                profile.getPeriod(),
                profile.getSamplesCount(),
                profile.getDroppedAttributesCount(),
                profile.getOriginalPayloadFormat(),
                profile.toByteArray());
    }

    /// Interprets an OTLP profiles export response as a [ConsumeResult].
    public static ConsumeResult<ProfilesData> result(ExportProfilesServiceResponse response) {
        var partial = response.getPartialSuccess();
        return CommonMapper.result(
                response.hasPartialSuccess(), partial.getRejectedProfiles(), partial.getErrorMessage());
    }

    // --- domain -> proto ---

    public static ExportProfilesServiceRequest toProto(ProfilesData profiles) {
        var request = ExportProfilesServiceRequest.newBuilder();
        for (var rp : profiles.resourceProfiles()) {
            request.addResourceProfiles(toProto(rp));
        }
        var dictionary = profiles.dictionary();
        if (dictionary.length > 0) {
            try {
                request.setDictionary(ProfilesDictionary.parseFrom(dictionary));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException("malformed opaque profiles dictionary payload", e);
            }
        }
        return request.build();
    }

    private static ResourceProfiles toProto(
            ProfilesData.ResourceProfiles rp) {
        var builder =
                ResourceProfiles.newBuilder()
                        .setResource(CommonMapper.toProtoResource(rp.resource()))
                        .setSchemaUrl(rp.schemaUrl());
        for (var sp : rp.scopeProfiles()) {
            builder.addScopeProfiles(toProto(sp));
        }
        return builder.build();
    }

    private static ScopeProfiles toProto(
            ProfilesData.ScopeProfiles sp) {
        var builder =
                ScopeProfiles.newBuilder()
                        .setScope(CommonMapper.toProtoScope(sp.scope()))
                        .setSchemaUrl(sp.schemaUrl());
        for (var profile : sp.profiles()) {
            builder.addProfiles(toProto(profile));
        }
        return builder.build();
    }

    private static Profile toProto(
            ProfilesData.Profile profile) {
        var rawProfile = profile.rawProfile();
        if (rawProfile.length > 0) {
            try {
                // Lossless passthrough: re-emit the captured bytes verbatim, preserving samples,
                // dictionary references, and the original payload.
                return Profile.parseFrom(rawProfile);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException("malformed opaque profile payload", e);
            }
        }
        // Scalar-only profile: rebuild the modeled metadata.
        return Profile.newBuilder()
                .setProfileId(CommonMapper.bytes(profile.profileId()))
                .setTimeUnixNano(profile.timeUnixNano())
                .setDurationNano(profile.durationNanos())
                .setPeriod(profile.period())
                .setDroppedAttributesCount(profile.droppedAttributesCount())
                .setOriginalPayloadFormat(profile.originalPayloadFormat())
                .build();
    }
}
