package dev.nthings.otlp4j.model;

import java.util.List;

/// A batch of profiling telemetry: the domain equivalent of an OTLP export request.
///
/// Profiles are still `v1development`; this model exposes only stable top-level profile metadata,
/// not sample/location/mapping/string tables or the shared dictionary.
@Experimental("profiles signal tracks OpenTelemetry v1development; shape may change without notice")
public record ProfilesData(List<ResourceProfiles> resourceProfiles) {

    public ProfilesData {
        resourceProfiles = List.copyOf(resourceProfiles);
    }

    /// Wraps `profiles` under one `resource` and `scope`.
    public static ProfilesData of(Resource resource, InstrumentationScope scope, List<Profile> profiles) {
        return new ProfilesData(
                List.of(new ResourceProfiles(resource, "", List.of(new ScopeProfiles(scope, "", profiles)))));
    }

    /// All profiles across every resource and scope, flattened for convenient consumption.
    public List<Profile> profiles() {
        return resourceProfiles.stream()
                .flatMap(rp -> rp.scopeProfiles().stream())
                .flatMap(sp -> sp.profiles().stream())
                .toList();
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

    /// Top-level metadata for a single profile. The profile id, when present, is a lowercase-hex
    /// string; it is empty for an id-less profile.
    public record Profile(
            String profileId,
            long timeUnixNano,
            long durationNanos,
            long period,
            int sampleCount,
            int droppedAttributesCount,
            String originalPayloadFormat) {}
}
