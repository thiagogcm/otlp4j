package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.signal.Payload;
import java.util.List;
import java.util.Objects;

/// A named set of alternative [MatchRule]s. A spec matches when any rule matches; [#none()] matches
/// nothing and [#all()] is an explicit catch-all. Compose [#of(MatchRule...)] for alternatives.
public final class MatchSpec {

    private static final MatchSpec NONE = new MatchSpec(List.of());
    private static final MatchSpec ALL = new MatchSpec(List.of(MatchRule.matchAll()));

    private final List<MatchRule> rules;

    private MatchSpec(List<MatchRule> rules) {
        this.rules = List.copyOf(rules);
    }

    /// Returns the shared empty spec, which matches nothing.
    public static MatchSpec none() {
        return NONE;
    }

    /// Returns a spec containing one explicit catch-all rule.
    public static MatchSpec all() {
        return ALL;
    }

    /// Creates a spec whose alternatives are the supplied rules.
    public static MatchSpec of(MatchRule... rules) {
        if (rules == null) {
            throw new IllegalArgumentException("rules must be non-null");
        }
        return new MatchSpec(List.of(rules));
    }

    /// Returns the immutable alternative rules.
    public List<MatchRule> rules() {
        return rules;
    }

    /// Returns whether any alternative matches the payload and context.
    public boolean matches(Payload<?> payload, ConsumeContext context) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(context, "context");
        for (var rule : rules) {
            if (rule.matches(payload, context)) {
                return true;
            }
        }
        return false;
    }
}
