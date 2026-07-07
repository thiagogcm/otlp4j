package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.components.internal.MatchSpec;
import dev.nthings.otlp4j.core.component.ComponentConfig;

/// The typed configuration for the filter processor: keep a payload when it matches `include` and does
/// not match `exclude`, otherwise drop it. An empty `include` admits every payload, since there is no
/// include filter configured; an empty `exclude` excludes nothing, since there is no exclude filter
/// configured. The default configuration is therefore a transparent pass-through that keeps every
/// payload.
///
/// @param include the spec a payload must match to be kept; empty admits every payload
/// @param exclude the spec that drops a payload it matches; empty excludes nothing
public record FilterConfig(MatchSpec include, MatchSpec exclude) implements ComponentConfig {

    /// Validates that both specs are present.
    public FilterConfig {
        if (include == null || exclude == null) {
            throw new IllegalArgumentException("include and exclude must be non-null; use MatchSpec.none()");
        }
    }

    /// The default configuration: an empty include and an empty exclude, a transparent pass-through
    /// that keeps every payload.
    ///
    /// @return the default configuration
    public static FilterConfig defaults() {
        return new FilterConfig(MatchSpec.none(), MatchSpec.none());
    }
}
