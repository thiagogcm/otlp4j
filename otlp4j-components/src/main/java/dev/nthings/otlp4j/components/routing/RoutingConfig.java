package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.components.internal.MatchSpec;
import dev.nthings.otlp4j.core.component.ComponentConfig;

/// The typed configuration for the routing processor: one branch of a content-routing fan-out. The
/// processor forwards a payload to its single downstream when `match` matches it; `defaultRoute` marks
/// the one designated branch that keeps the remainder left over once every other branch's own `match`
/// has been checked, by treating an unset (empty) `match` as matching everything rather than matching
/// nothing.
///
/// Because a routing processor only ever sees its own configuration, a `defaultRoute` branch with a
/// non-empty `match` still evaluates that match normally; the flag only changes what an empty `match`
/// means. Composing a full content-routing story is therefore the config author's responsibility:
/// giving the specific branches disjoint match conditions and leaving the designated default branch's
/// `match` empty is what makes each payload reach exactly one specific branch, with the default branch
/// also receiving every payload as the safety-net remainder.
///
/// @param match        the condition this branch forwards on; empty means "no explicit condition"
/// @param defaultRoute whether an empty `match` means "forward everything" (true) or "forward nothing"
///                     (false, the ordinary branch default)
public record RoutingConfig(MatchSpec match, boolean defaultRoute) implements ComponentConfig {

    /// Validates that the match spec is present.
    public RoutingConfig {
        if (match == null) {
            throw new IllegalArgumentException("match must be non-null; use MatchSpec.none()");
        }
    }

    /// The default configuration: an empty match with `defaultRoute` true, so an unconfigured routing
    /// processor forwards every payload rather than silently dropping everything.
    ///
    /// @return the default configuration
    public static RoutingConfig defaults() {
        return new RoutingConfig(MatchSpec.none(), true);
    }
}
