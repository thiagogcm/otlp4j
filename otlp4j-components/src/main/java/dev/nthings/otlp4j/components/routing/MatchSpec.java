package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.Attributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/// A set of alternative [MatchRule]s. A spec matches a resource group when any rule matches it;
/// [#none()] matches nothing and [#all()] is an explicit catch-all. Compose [#of(MatchRule...)] for
/// alternatives. A spec is a configuration value: [FilterProcessor] evaluates it.
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

    /// Starts matching one payload and returns the test that decides each of its resource groups.
    ///
    /// The returned test takes one resource group's attributes, as listed by the payload's
    /// [routing metadata][dev.nthings.otlp4j.core.signal.RoutingMetadata#resources()], and reports
    /// whether any alternative matches that group. Header conditions are evaluated here, once. A
    /// domain predicate runs at most once, and only when a resource group satisfies the rest of its
    /// rule. Use the test for this payload only, from one thread.
    ///
    /// @param payload the payload whose resource groups will be tested
    /// @param context the request context supplying client headers
    /// @return the per-resource test
    Predicate<Attributes> resourceMatcher(Payload<?> payload, ConsumeContext context) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(context, "context");
        var candidates = new ArrayList<MatchRule>(rules.size());
        for (var rule : rules) {
            if (rule.matchesHeaders(context)) {
                candidates.add(rule);
            }
        }
        var domainOutcomes = new Boolean[candidates.size()];
        return resource -> {
            Objects.requireNonNull(resource, "resource");
            for (var i = 0; i < candidates.size(); i++) {
                var rule = candidates.get(i);
                if (!rule.matchesResource(resource)) {
                    continue;
                }
                if (domainOutcomes[i] == null) {
                    domainOutcomes[i] = rule.matchesDomain(payload);
                }
                if (domainOutcomes[i]) {
                    return true;
                }
            }
            return false;
        };
    }
}
