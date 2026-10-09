package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.component.ComponentConfig;

/// The typed configuration for the filter processor: keep a resource group when it matches `include`
/// and does not match `exclude`, otherwise drop it. Use [MatchSpec#all()] for an explicit pass-through
/// include and [MatchSpec#none()] for an include that keeps nothing. An empty `exclude` excludes
/// nothing. The default configuration is therefore a transparent pass-through that keeps everything.
///
/// @param include the spec a resource group must match to be kept
/// @param exclude the spec that drops a resource group it matches; empty excludes nothing
public record FilterConfig(MatchSpec include, MatchSpec exclude) implements ComponentConfig {

    /// Validates that both specs are present.
    public FilterConfig {
        if (include == null || exclude == null) {
            throw new IllegalArgumentException("include and exclude must be non-null; use MatchSpec.all() or none()");
        }
    }

    /// The default configuration: an explicit catch-all include and an empty exclude, a transparent
    /// pass-through that keeps everything.
    ///
    /// @return the default configuration
    public static FilterConfig defaults() {
        return new FilterConfig(MatchSpec.all(), MatchSpec.none());
    }

    /// A configuration that keeps what `match` selects and excludes nothing: the condition of one
    /// branch when several pipelines route a shared source.
    ///
    /// @param match the spec a resource group must match to be kept
    /// @return the configuration
    public static FilterConfig keep(MatchSpec match) {
        return new FilterConfig(match, MatchSpec.none());
    }
}
