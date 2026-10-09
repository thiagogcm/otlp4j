package dev.nthings.otlp4j.components.routing;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.TracesData;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/// Proves the spec's disjunction semantics per resource group: it matches a group when any one of its
/// alternative rules matches that group, and an empty spec matches nothing.
class MatchSpecTest {

    private static final TestConsumeContext CTX = new TestConsumeContext();
    private static final Attributes CHECKOUT = service("checkout");
    private static final Attributes BILLING = service("billing");

    @Test
    void emptySpecMatchesNothing() {
        assertThat(MatchSpec.none().resourceMatcher(traces(1), CTX).test(CHECKOUT)).isFalse();
    }

    @Test
    void matchAllSpecMatchesEverything() {
        var matcher = MatchSpec.of(MatchRule.matchAll()).resourceMatcher(traces(1), CTX);

        assertThat(matcher.test(CHECKOUT)).isTrue();
        assertThat(matcher.test(Attributes.empty())).isTrue();
    }

    @Test
    void specMatchesAGroupWhenAnyOneOfSeveralAlternativeRulesMatchesIt() {
        var spec = MatchSpec.of(serviceRule("billing"), serviceRule("checkout"));

        assertThat(spec.resourceMatcher(traces(1), CTX).test(CHECKOUT)).isTrue();
    }

    @Test
    void specMatchesNothingWhenNoAlternativeRuleMatches() {
        var spec = MatchSpec.of(serviceRule("billing"), serviceRule("payments"));

        assertThat(spec.resourceMatcher(traces(1), CTX).test(CHECKOUT)).isFalse();
    }

    @Test
    void oneMatcherDecidesEachResourceGroupOnItsOwnAttributes() {
        var matcher = MatchSpec.of(serviceRule("billing")).resourceMatcher(traces(1), CTX);

        assertThat(matcher.test(CHECKOUT)).isFalse();
        assertThat(matcher.test(BILLING)).isTrue();
        assertThat(matcher.test(Attributes.empty())).isFalse();
    }

    @Test
    void aNonStringAttributeDoesNotSatisfyAResourceCondition() {
        var numeric = Attributes.builder().put("service.name", AttributeValue.of(7L)).build();
        var matcher = MatchSpec.of(serviceRule("7")).resourceMatcher(traces(1), CTX);

        assertThat(matcher.test(numeric)).isFalse();
    }

    @Test
    void aDomainPredicateRunsOnceHoweverManyResourceGroupsReachIt() {
        var calls = new AtomicInteger();
        var rule = MatchRule.builder()
                .resourceAttributeRegex("service.name", "checkout|billing")
                .domainPredicate(batch -> {
                    calls.incrementAndGet();
                    return ((TracesData) batch).spanCount() == 1;
                })
                .build();
        var matcher = MatchSpec.of(rule).resourceMatcher(traces(1), CTX);

        assertThat(matcher.test(CHECKOUT)).isTrue();
        assertThat(matcher.test(BILLING)).isTrue();
        assertThat(matcher.test(service("payments"))).isFalse();
        assertThat(calls).hasValue(1);
    }

    private static MatchRule serviceRule(String service) {
        return MatchRule.builder().resourceAttribute("service.name", service).build();
    }

    private static Attributes service(String name) {
        return Attributes.builder().put("service.name", name).build();
    }
}
