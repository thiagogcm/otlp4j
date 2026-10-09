package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// A batch of trace telemetry: the domain equivalent of an OTLP `ExportTraceServiceRequest`.
///
/// The hierarchy mirrors the protocol - `TracesData → ResourceSpans → ScopeSpans → Span` -
/// but every type here is a plain typed record with no dependency on the generated proto classes.
public record TracesData(List<ResourceSpans> resourceSpans) {

    public TracesData {
        resourceSpans = List.copyOf(resourceSpans);
    }

    /// Wraps `spans` under one `resource` and `scope`.
    public static TracesData of(Resource resource, InstrumentationScope scope, List<Span> spans) {
        return new TracesData(List.of(new ResourceSpans(resource, "", List.of(new ScopeSpans(scope, "", spans)))));
    }

    /// All spans across every resource and scope, flattened for convenient consumption.
    ///
    /// Allocates a fresh list on every call; on a hot path prefer [#forEachSpan] or [#spanCount].
    public List<Span> spans() {
        return resourceSpans.stream()
                .flatMap(rs -> rs.scopeSpans().stream())
                .flatMap(ss -> ss.spans().stream())
                .toList();
    }

    /// Applies `action` to every span across every resource and scope, in [#spans] order without
    /// allocating the flattened list.
    public void forEachSpan(Consumer<? super Span> action) {
        Objects.requireNonNull(action, "action");
        for (var resource : resourceSpans) {
            for (var scope : resource.scopeSpans()) {
                for (var span : scope.spans()) {
                    action.accept(span);
                }
            }
        }
    }

    /// The total number of spans across every resource and scope, counted without allocating the
    /// list [#spans] builds.
    public int spanCount() {
        var count = 0;
        for (var resource : resourceSpans) {
            for (var scope : resource.scopeSpans()) {
                count += scope.spans().size();
            }
        }
        return count;
    }

    /// Returns this batch with every resource replaced by `mapper`'s result. Schema URLs, scopes, and
    /// spans are retained. Returns this instance when `mapper` returns each resource unchanged.
    public TracesData mapResources(UnaryOperator<Resource> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return withResourceSpans(Rewrites.map(resourceSpans, group -> group.withResource(
                Objects.requireNonNull(mapper.apply(group.resource()), "mapper returned null"))));
    }

    /// Returns this batch with only the resource groups whose resource `keep` accepts. A rejected
    /// group is removed together with its spans. Returns this instance when `keep` accepts every
    /// resource.
    public TracesData filterResources(Predicate<? super Resource> keep) {
        Objects.requireNonNull(keep, "keep");
        return withResourceSpans(Rewrites.filter(resourceSpans, group -> keep.test(group.resource())));
    }

    /// Returns this batch with every span replaced by `mapper`'s result, which must be non-null.
    /// Resource and scope grouping is retained. Returns this instance when `mapper` returns each
    /// span unchanged.
    public TracesData mapSpans(UnaryOperator<Span> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return withResourceSpans(Rewrites.map(resourceSpans, resource -> resource.withScopeSpans(
                Rewrites.map(resource.scopeSpans(), scope -> scope.withSpans(
                        Rewrites.map(scope.spans(), mapper))))));
    }

    /// Returns this batch with only the spans `keep` accepts, in their original order and grouping. A
    /// scope or resource that loses all its spans is removed, together with any empty scopes it held;
    /// groups that lose nothing are kept as they are. Returns this instance when `keep` accepts every
    /// span.
    public TracesData filterSpans(Predicate<? super Span> keep) {
        Objects.requireNonNull(keep, "keep");
        return withResourceSpans(Rewrites.prune(
                resourceSpans,
                resource -> resource.withScopeSpans(Rewrites.prune(
                        resource.scopeSpans(),
                        scope -> scope.withSpans(Rewrites.filter(scope.spans(), keep)),
                        scope -> scope.spans().isEmpty())),
                resource -> resource.scopeSpans().stream().allMatch(scope -> scope.spans().isEmpty())));
    }

    private TracesData withResourceSpans(List<ResourceSpans> replacement) {
        return replacement == resourceSpans ? this : new TracesData(replacement);
    }

    /// Spans from one [Resource], grouped by instrumentation scope.
    public record ResourceSpans(Resource resource, String schemaUrl, List<ScopeSpans> scopeSpans) {
        public ResourceSpans {
            scopeSpans = List.copyOf(scopeSpans);
        }

        ResourceSpans withResource(Resource replacement) {
            return replacement == resource ? this : new ResourceSpans(replacement, schemaUrl, scopeSpans);
        }

        ResourceSpans withScopeSpans(List<ScopeSpans> replacement) {
            return replacement == scopeSpans ? this : new ResourceSpans(resource, schemaUrl, replacement);
        }
    }

    /// Spans produced by a single [InstrumentationScope].
    public record ScopeSpans(InstrumentationScope scope, String schemaUrl, List<Span> spans) {
        public ScopeSpans {
            spans = List.copyOf(spans);
        }

        ScopeSpans withSpans(List<Span> replacement) {
            return replacement == spans ? this : new ScopeSpans(scope, schemaUrl, replacement);
        }
    }
}
