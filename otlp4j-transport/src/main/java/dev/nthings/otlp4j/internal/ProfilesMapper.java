package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import java.util.ArrayList;
import java.util.List;

/// Maps profiles between generated proto types and [ProfilesData].
///
/// Profiles are `v1development`; only top-level metadata is modeled, so domain-to-proto mapping is
/// lossy for sample/location/mapping/string tables and the shared dictionary.
final class ProfilesMapper {

    private ProfilesMapper() {}

    // --- proto -> domain ---------------------------------------------------------------------

    public static ProfilesData toDomain(ExportProfilesServiceRequest request) {
        var resourceProfiles =
                new ArrayList<ProfilesData.ResourceProfiles>(request.getResourceProfilesCount());
        for (var rp : request.getResourceProfilesList()) {
            resourceProfiles.add(toDomain(rp));
        }
        return new ProfilesData(resourceProfiles);
    }

    private static ProfilesData.ResourceProfiles toDomain(
            io.opentelemetry.proto.profiles.v1development.ResourceProfiles rp) {
        var scopeProfiles =
                new ArrayList<ProfilesData.ScopeProfiles>(rp.getScopeProfilesCount());
        for (var sp : rp.getScopeProfilesList()) {
            scopeProfiles.add(toDomain(sp));
        }
        return new ProfilesData.ResourceProfiles(
                CommonMapper.resource(rp.getResource()), rp.getSchemaUrl(), scopeProfiles);
    }

    private static ProfilesData.ScopeProfiles toDomain(
            io.opentelemetry.proto.profiles.v1development.ScopeProfiles sp) {
        var profiles = new ArrayList<ProfilesData.Profile>(sp.getProfilesCount());
        for (var profile : sp.getProfilesList()) {
            profiles.add(toDomain(profile));
        }
        return new ProfilesData.ScopeProfiles(
                CommonMapper.scope(sp.getScope()), sp.getSchemaUrl(), profiles);
    }

    private static ProfilesData.Profile toDomain(
            io.opentelemetry.proto.profiles.v1development.Profile profile) {
        return new ProfilesData.Profile(
                CommonMapper.hex(profile.getProfileId()),
                profile.getTimeUnixNano(),
                profile.getDurationNano(),
                profile.getPeriod(),
                profile.getSamplesCount(),
                profile.getDroppedAttributesCount(),
                profile.getOriginalPayloadFormat());
    }

    /// Interprets an OTLP profiles export response as a [ConsumeResult].
    public static ConsumeResult<ProfilesData> result(ExportProfilesServiceResponse response) {
        if (!response.hasPartialSuccess()) {
            return ConsumeResult.accepted();
        }
        var partial = response.getPartialSuccess();
        if (partial.getRejectedProfiles() == 0 && partial.getErrorMessage().isEmpty()) {
            return ConsumeResult.accepted();
        }
        if (partial.getRejectedProfiles() == 0) {
            return ConsumeResult.rejected(partial.getErrorMessage());
        }
        return ConsumeResult.partial(partial.getRejectedProfiles(), partial.getErrorMessage());
    }

    // --- domain -> proto ---------------------------------------------------------------------

    public static ExportProfilesServiceRequest toProto(ProfilesData profiles) {
        var request = ExportProfilesServiceRequest.newBuilder();
        for (var rp : profiles.resourceProfiles()) {
            request.addResourceProfiles(toProto(rp));
        }
        return request.build();
    }

    private static io.opentelemetry.proto.profiles.v1development.ResourceProfiles toProto(
            ProfilesData.ResourceProfiles rp) {
        var builder =
                io.opentelemetry.proto.profiles.v1development.ResourceProfiles.newBuilder()
                        .setResource(CommonMapper.toProtoResource(rp.resource()))
                        .setSchemaUrl(rp.schemaUrl());
        for (var sp : rp.scopeProfiles()) {
            builder.addScopeProfiles(toProto(sp));
        }
        return builder.build();
    }

    private static io.opentelemetry.proto.profiles.v1development.ScopeProfiles toProto(
            ProfilesData.ScopeProfiles sp) {
        var builder =
                io.opentelemetry.proto.profiles.v1development.ScopeProfiles.newBuilder()
                        .setScope(CommonMapper.toProtoScope(sp.scope()))
                        .setSchemaUrl(sp.schemaUrl());
        for (var profile : sp.profiles()) {
            builder.addProfiles(toProto(profile));
        }
        return builder.build();
    }

    private static io.opentelemetry.proto.profiles.v1development.Profile toProto(
            ProfilesData.Profile profile) {
        return io.opentelemetry.proto.profiles.v1development.Profile.newBuilder()
                .setProfileId(CommonMapper.bytes(profile.profileId()))
                .setTimeUnixNano(profile.timeUnixNano())
                .setDurationNano(profile.durationNanos())
                .setPeriod(profile.period())
                .setDroppedAttributesCount(profile.droppedAttributesCount())
                .setOriginalPayloadFormat(profile.originalPayloadFormat())
                .build();
    }
}
