package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/// A batch of trace telemetry: the domain equivalent of an OTLP `ExportTraceServiceRequest`.
///
/// The hierarchy mirrors the protocol — `TracesData → ResourceSpans → ScopeSpans → Span` —
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

    /// Spans from one [Resource], grouped by instrumentation scope.
    public record ResourceSpans(Resource resource, String schemaUrl, List<ScopeSpans> scopeSpans) {
        public ResourceSpans {
            scopeSpans = List.copyOf(scopeSpans);
        }
    }

    /// Spans produced by a single [InstrumentationScope].
    public record ScopeSpans(InstrumentationScope scope, String schemaUrl, List<Span> spans) {
        public ScopeSpans {
            spans = List.copyOf(spans);
        }
    }
}
