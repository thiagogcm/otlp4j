package dev.nthings.otlp4j.model;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/// A batch of profiling telemetry: the domain equivalent of an OTLP export request.
///
/// Profiles are still `v1development`. Top-level profile metadata is exposed for inspection;
/// profile payloads are forwarded losslessly via opaque passthrough - the shared [dictionary]
/// and each [Profile]'s raw bytes re-emit byte-for-byte.
@Experimental("profiles signal tracks OpenTelemetry v1development; shape may change without notice")
public record ProfilesData(List<ResourceProfiles> resourceProfiles, byte[] dictionary) {

    private static final byte[] EMPTY_BYTES = new byte[0];

    public ProfilesData {
        resourceProfiles = List.copyOf(resourceProfiles);
        dictionary = dictionary == null ? EMPTY_BYTES : dictionary.clone();
    }

    /// Wraps `profiles` under one `resource` and `scope`, with no shared dictionary.
    public static ProfilesData of(Resource resource, InstrumentationScope scope, List<Profile> profiles) {
        return new ProfilesData(
                List.of(new ResourceProfiles(resource, "", List.of(new ScopeProfiles(scope, "", profiles)))),
                EMPTY_BYTES);
    }

    /// The opaque serialized bytes of the proto `ProfilesDictionary` shared across the batch, or an
    /// empty array when none is present. Treat as opaque and do not mutate; the returned array is a
    /// defensive clone.
    @Override
    public byte[] dictionary() {
        return dictionary.clone();
    }

    /// All profiles across every resource and scope, flattened for convenient consumption.
    ///
    /// Allocates a fresh list on every call; on a hot path prefer [#forEachProfile] or
    /// [#profileCount].
    public List<Profile> profiles() {
        return resourceProfiles.stream()
                .flatMap(rp -> rp.scopeProfiles().stream())
                .flatMap(sp -> sp.profiles().stream())
                .toList();
    }

    /// Applies `action` to every profile across every resource and scope, in [#profiles] order
    /// without allocating the flattened list.
    public void forEachProfile(Consumer<? super Profile> action) {
        Objects.requireNonNull(action, "action");
        for (var resource : resourceProfiles) {
            for (var scope : resource.scopeProfiles()) {
                for (var profile : scope.profiles()) {
                    action.accept(profile);
                }
            }
        }
    }

    /// The total number of profiles across every resource and scope, counted without allocating the
    /// list [#profiles] builds.
    public int profileCount() {
        var count = 0;
        for (var resource : resourceProfiles) {
            for (var scope : resource.scopeProfiles()) {
                count += scope.profiles().size();
            }
        }
        return count;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ProfilesData other
                && resourceProfiles.equals(other.resourceProfiles)
                && Arrays.equals(dictionary, other.dictionary);
    }

    @Override
    public int hashCode() {
        return 31 * resourceProfiles.hashCode() + Arrays.hashCode(dictionary);
    }

    @Override
    public String toString() {
        return "ProfilesData[resourceProfiles=" + resourceProfiles
                + ", dictionary=" + dictionary.length + " bytes]";
    }

    /// Profiles from one [Resource], grouped by instrumentation scope.
    public record ResourceProfiles(
            Resource resource, String schemaUrl, List<ScopeProfiles> scopeProfiles) {
        public ResourceProfiles {
            scopeProfiles = List.copyOf(scopeProfiles);
        }
    }

    /// Profiles produced by a single [InstrumentationScope].
    public record ScopeProfiles(
            InstrumentationScope scope, String schemaUrl, List<Profile> profiles) {
        public ScopeProfiles {
            profiles = List.copyOf(profiles);
        }
    }

    /// Top-level metadata for a single profile, plus its opaque raw bytes for lossless forwarding.
    ///
    /// The scalar fields are best-effort inspection metadata; the profile id, when present, is a
    /// lowercase-hex string and is empty for an id-less profile. When [rawProfile] is present (the
    /// serialized proto `Profile`) the profile re-emits byte-identically, preserving samples,
    /// dictionary references, and the original payload. A profile built from scalars only (empty
    /// [rawProfile]) re-emits just the scalar metadata.
    public record Profile(
            String profileId,
            long timeUnixNano,
            long durationNanos,
            long period,
            int sampleCount,
            int droppedAttributesCount,
            String originalPayloadFormat,
            byte[] rawProfile) {

        public Profile {
            rawProfile = rawProfile == null ? EMPTY_BYTES : rawProfile.clone();
        }

        /// The serialized proto `Profile` bytes, or an empty array when only scalar metadata is
        /// modeled. Treat as opaque and do not mutate; the returned array is a defensive clone.
        @Override
        public byte[] rawProfile() {
            return rawProfile.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Profile other
                    && Objects.equals(profileId, other.profileId)
                    && timeUnixNano == other.timeUnixNano
                    && durationNanos == other.durationNanos
                    && period == other.period
                    && sampleCount == other.sampleCount
                    && droppedAttributesCount == other.droppedAttributesCount
                    && Objects.equals(originalPayloadFormat, other.originalPayloadFormat)
                    && Arrays.equals(rawProfile, other.rawProfile);
        }

        @Override
        public int hashCode() {
            var result = Objects.hashCode(profileId);
            result = 31 * result + Long.hashCode(timeUnixNano);
            result = 31 * result + Long.hashCode(durationNanos);
            result = 31 * result + Long.hashCode(period);
            result = 31 * result + sampleCount;
            result = 31 * result + droppedAttributesCount;
            result = 31 * result + Objects.hashCode(originalPayloadFormat);
            result = 31 * result + Arrays.hashCode(rawProfile);
            return result;
        }

        @Override
        public String toString() {
            return "Profile[profileId=" + profileId
                    + ", timeUnixNano=" + timeUnixNano
                    + ", durationNanos=" + durationNanos
                    + ", period=" + period
                    + ", sampleCount=" + sampleCount
                    + ", droppedAttributesCount=" + droppedAttributesCount
                    + ", originalPayloadFormat=" + originalPayloadFormat
                    + ", rawProfile=" + rawProfile.length + " bytes]";
        }
    }
}
