package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.component.ComponentConfig;

/// The typed configuration for the routing processor: one branch of a content-routing fan-out. The
/// processor forwards a payload to its single downstream when `match` matches it. A catch-all branch is
/// explicit: use `MatchSpec.of(MatchRule.matchAll())`; an empty spec matches nothing.
///
/// @param match the condition this branch forwards on; empty means "forward nothing"
public record RoutingConfig(MatchSpec match) implements ComponentConfig {

    /// Validates that the match spec is present.
    public RoutingConfig {
        if (match == null) {
            throw new IllegalArgumentException("match must be non-null; use MatchSpec.none()");
        }
    }

    /// The default configuration: an empty match that forwards nothing until a rule is configured.
    ///
    /// @return the default configuration
    public static RoutingConfig defaults() {
        return new RoutingConfig(MatchSpec.none());
    }
}
