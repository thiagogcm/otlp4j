package dev.nthings.otlp4j.components.internal;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.List;

/// A named set of alternative [MatchRule]s: a spec matches a payload when any one of its rules matches,
/// the disjunction over each rule's own conjunction of conditions. An empty spec (no rules at all)
/// matches nothing, the vacuous case of "none of zero alternatives holds"; [MatchRule#matchAll] is the
/// explicit building block for a rule that matches everything, composed into a spec when a catch-all
/// alternative is wanted.
///
/// This is the shape a Collector-style include or exclude filter binds from configuration: each
/// resource-attribute or header condition map under one filter position folds into one rule, so a spec
/// built from configuration today carries at most one rule, with room to grow into several alternative
/// rules for a future richer configuration shape without changing this type.
public final class MatchSpec {

    private static final MatchSpec NONE = new MatchSpec(List.of());

    private final List<MatchRule> rules;

    private MatchSpec(List<MatchRule> rules) {
        this.rules = List.copyOf(rules);
    }

    /// The empty spec, matching nothing.
    ///
    /// @return the shared empty spec
    public static MatchSpec none() {
        return NONE;
    }

    /// A spec over the given alternative rules, any one of which matching is enough for the spec to
    /// match.
    ///
    /// @param rules the alternative rules
    /// @return the spec
    public static MatchSpec of(MatchRule... rules) {
        return new MatchSpec(List.of(rules));
    }

    /// The alternative rules this spec holds.
    ///
    /// @return the rules, possibly empty
    public List<MatchRule> rules() {
        return rules;
    }

    /// Whether any one of this spec's rules matches `payload` under `context`.
    ///
    /// @param payload the payload to test
    /// @param context the consume context to test
    /// @return true when at least one rule matches, false when the spec is empty or no rule matches
    public boolean matches(TelemetryPayload payload, ConsumeContext context) {
        for (var rule : rules) {
            if (rule.matches(payload, context)) {
                return true;
            }
        }
        return false;
    }
}
