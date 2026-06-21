package dev.nthings.otlp4j.model;

import java.util.List;

/// A batch of trace telemetry: the domain equivalent of an OTLP `ExportTraceServiceRequest`.
///
/// The hierarchy mirrors the protocol — `TraceData → ResourceSpans → ScopeSpans → Span` —
/// but every type here is a plain typed record with no dependency on the generated proto classes.
public record TraceData(List<ResourceSpans> resourceSpans) {

    public TraceData {
        resourceSpans = List.copyOf(resourceSpans);
    }

    /// Wraps `spans` under one `resource` and `scope`.
    public static TraceData of(Resource resource, InstrumentationScope scope, List<Span> spans) {
        return new TraceData(List.of(new ResourceSpans(resource, "", List.of(new ScopeSpans(scope, "", spans)))));
    }

    /// All spans across every resource and scope, flattened for convenient consumption.
    public List<Span> spans() {
        return resourceSpans.stream()
                .flatMap(rs -> rs.scopeSpans().stream())
                .flatMap(ss -> ss.spans().stream())
                .toList();
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
