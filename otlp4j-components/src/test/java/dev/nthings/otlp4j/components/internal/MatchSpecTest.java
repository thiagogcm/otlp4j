package dev.nthings.otlp4j.components.internal;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.components.routing.MatchRule;
import dev.nthings.otlp4j.components.routing.MatchSpec;
import org.junit.jupiter.api.Test;

/// Proves the spec's disjunction semantics: it matches when any one of its alternative rules matches,
/// and an empty spec matches nothing.
class MatchSpecTest {

    private static final TestConsumeContext CTX = new TestConsumeContext();

    @Test
    void emptySpecMatchesNothing() {
        assertThat(MatchSpec.none().matches(traces(1), CTX)).isFalse();
    }

    @Test
    void matchAllSpecMatchesEverything() {
        assertThat(MatchSpec.of(MatchRule.matchAll()).matches(traces(1), CTX)).isTrue();
    }

    @Test
    void specMatchesWhenAnyOneOfSeveralAlternativeRulesMatches() {
        var checkout = MatchRule.builder().resourceAttribute("service.name", "checkout").build();
        var billing = MatchRule.builder().resourceAttribute("service.name", "billing").build();
        var spec = MatchSpec.of(billing, checkout);

        assertThat(spec.matches(traces(1), CTX)).isTrue();
    }

    @Test
    void specMatchesNothingWhenNoAlternativeRuleMatches() {
        var billing = MatchRule.builder().resourceAttribute("service.name", "billing").build();
        var payments = MatchRule.builder().resourceAttribute("service.name", "payments").build();
        var spec = MatchSpec.of(billing, payments);

        assertThat(spec.matches(traces(1), CTX)).isFalse();
    }
}
