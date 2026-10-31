package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// A batch of profiling telemetry: the domain equivalent of an OTLP export request.
///
/// Experimental. Profiles are still `v1development`; this type's shape may change or be removed in a
/// future minor release without notice. Every [Profile] holds resolved values, so a batch has no shared
/// dictionary to keep consistent: the codec builds one when the batch is encoded.
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

    /// Returns this batch with every resource replaced by `mapper`'s result. Schema URLs, scopes, and
    /// profiles are retained. Returns this instance when `mapper` returns each resource unchanged.
    public ProfilesData mapResources(UnaryOperator<Resource> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return withResourceProfiles(Rewrites.map(resourceProfiles, group -> group.withResource(
                Objects.requireNonNull(mapper.apply(group.resource()), "mapper returned null"))));
    }

    /// Returns this batch with only the resource groups whose resource `keep` accepts. A rejected
    /// group is removed together with its profiles. Returns this instance when `keep` accepts every
    /// resource.
    public ProfilesData filterResources(Predicate<? super Resource> keep) {
        Objects.requireNonNull(keep, "keep");
        return withResourceProfiles(Rewrites.filter(resourceProfiles, group -> keep.test(group.resource())));
    }

    /// Returns this batch with every profile replaced by `mapper`'s result, which must be non-null.
    /// Resource and scope grouping are retained. Returns this instance when `mapper` returns each
    /// profile unchanged.
    public ProfilesData mapProfiles(UnaryOperator<Profile> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return withResourceProfiles(Rewrites.map(resourceProfiles, resource -> resource.withScopeProfiles(
                Rewrites.map(resource.scopeProfiles(), scope -> scope.withProfiles(
                        Rewrites.map(scope.profiles(), mapper))))));
    }

    /// Returns this batch with only the profiles `keep` accepts, in their original order and grouping.
    /// A scope or resource that loses all its profiles is removed, together with any empty scopes it
    /// held; groups that lose nothing are kept as they are. Returns this instance when `keep` accepts
    /// every profile.
    public ProfilesData filterProfiles(Predicate<? super Profile> keep) {
        Objects.requireNonNull(keep, "keep");
        return withResourceProfiles(Rewrites.prune(
                resourceProfiles,
                resource -> resource.withScopeProfiles(Rewrites.prune(
                        resource.scopeProfiles(),
                        scope -> scope.withProfiles(Rewrites.filter(scope.profiles(), keep)),
                        scope -> scope.profiles().isEmpty())),
                resource -> resource.scopeProfiles().stream().allMatch(scope -> scope.profiles().isEmpty())));
    }

    private ProfilesData withResourceProfiles(List<ResourceProfiles> replacement) {
        return replacement == resourceProfiles ? this : new ProfilesData(replacement);
    }

    /// Profiles from one [Resource], grouped by instrumentation scope.
    public record ResourceProfiles(
            Resource resource, String schemaUrl, List<ScopeProfiles> scopeProfiles) {
        public ResourceProfiles {
            scopeProfiles = List.copyOf(scopeProfiles);
        }

        ResourceProfiles withResource(Resource replacement) {
            return replacement == resource ? this : new ResourceProfiles(replacement, schemaUrl, scopeProfiles);
        }

        ResourceProfiles withScopeProfiles(List<ScopeProfiles> replacement) {
            return replacement == scopeProfiles ? this : new ResourceProfiles(resource, schemaUrl, replacement);
        }
    }

    /// Profiles produced by a single [InstrumentationScope].
    public record ScopeProfiles(
            InstrumentationScope scope, String schemaUrl, List<Profile> profiles) {
        public ScopeProfiles {
            profiles = List.copyOf(profiles);
        }

        ScopeProfiles withProfiles(List<Profile> replacement) {
            return replacement == profiles ? this : new ScopeProfiles(scope, schemaUrl, replacement);
        }
    }
}
