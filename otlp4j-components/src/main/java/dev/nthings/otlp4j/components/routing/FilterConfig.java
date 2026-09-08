package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.component.ComponentConfig;

/// The typed configuration for the filter processor: keep a payload when it matches `include` and does
/// not match `exclude`, otherwise drop it. Use [MatchSpec#all()] for an explicit pass-through include
/// and [MatchSpec#none()] for an include that keeps nothing. An empty `exclude` excludes nothing. The
/// default configuration is therefore a transparent pass-through that keeps every payload.
///
/// @param include the spec a payload must match to be kept
/// @param exclude the spec that drops a payload it matches; empty excludes nothing
public record FilterConfig(MatchSpec include, MatchSpec exclude) implements ComponentConfig {

    /// Validates that both specs are present.
    public FilterConfig {
        if (include == null || exclude == null) {
            throw new IllegalArgumentException("include and exclude must be non-null; use MatchSpec.all() or none()");
        }
    }

    /// The default configuration: an explicit catch-all include and an empty exclude, a transparent
    /// pass-through that keeps every payload.
    ///
    /// @return the default configuration
    public static FilterConfig defaults() {
        return new FilterConfig(MatchSpec.all(), MatchSpec.none());
    }
}
