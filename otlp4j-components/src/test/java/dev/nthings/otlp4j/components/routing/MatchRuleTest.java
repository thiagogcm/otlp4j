package dev.nthings.otlp4j.components.routing;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.components.support.MaterialisationTrackingPayload;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.TracesData;
import org.junit.jupiter.api.Test;

/// Proves the shared match engine's conjunction semantics over resource attributes, headers, and the
/// domain-predicate escape hatch, and that only the escape hatch forces domain materialisation. Every
/// payload here carries one `checkout` resource group, which is the group each rule is tested against.
class MatchRuleTest {

    private static final TestConsumeContext EMPTY_CONTEXT = new TestConsumeContext();

    @Test
    void matchAllMatchesEveryPayloadRegardlessOfContext() {
        var rule = MatchRule.matchAll();
        assertThat(matches(rule, traces(1), EMPTY_CONTEXT)).isTrue();
        assertThat(matches(rule, traces(1), TestConsumeContext.withHeader("x", "y"))).isTrue();
    }

    @Test
    void resourceAttributeExactMatch() {
        var rule = MatchRule.builder().resourceAttribute("service.name", "checkout").build();
        assertThat(matches(rule, traces(1), EMPTY_CONTEXT)).isTrue();
    }

    @Test
    void resourceAttributeExactMismatchFails() {
        var rule = MatchRule.builder().resourceAttribute("service.name", "billing").build();
        assertThat(matches(rule, traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void resourceAttributeAbsentKeyFails() {
        var rule = MatchRule.builder().resourceAttribute("does.not.exist", "anything").build();
        assertThat(matches(rule, traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void resourceAttributeRegexMatch() {
        var matching = MatchRule.builder().resourceAttributeRegex("service.name", "check.*").build();
        var mismatching = MatchRule.builder().resourceAttributeRegex("service.name", "^other$").build();

        assertThat(matches(matching, traces(1), EMPTY_CONTEXT)).isTrue();
        assertThat(matches(mismatching, traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void resourceAttributeRegexIsAFullMatchNotASubstringSearch() {
        // "check" alone would match a substring search but must not satisfy a full match against
        // "checkout".
        var rule = MatchRule.builder().resourceAttributeRegex("service.name", "check").build();
        assertThat(matches(rule, traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void headerExactMatch() {
        var rule = MatchRule.builder().header("x-tenant", "acme").build();

        assertThat(matches(rule, traces(1), TestConsumeContext.withHeader("x-tenant", "acme"))).isTrue();
        assertThat(matches(rule, traces(1), TestConsumeContext.withHeader("x-tenant", "other"))).isFalse();
        assertThat(matches(rule, traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void headerMatchesWhenAnyOfSeveralValuesSatisfiesTheCondition() {
        var rule = MatchRule.builder().header("x-tenant", "b").build();
        var context = TestConsumeContext.withHeader("x-tenant", "a", "b", "c");
        assertThat(matches(rule, traces(1), context)).isTrue();
    }

    @Test
    void headerRegexMatch() {
        var rule = MatchRule.builder().headerRegex("x-tenant", "acme-.*").build();
        assertThat(matches(rule, traces(1), TestConsumeContext.withHeader("x-tenant", "acme-prod"))).isTrue();
        assertThat(matches(rule, traces(1), TestConsumeContext.withHeader("x-tenant", "other"))).isFalse();
    }

    @Test
    void conjunctionRequiresEveryConfiguredConditionToHold() {
        var rule = MatchRule.builder()
                .resourceAttribute("service.name", "checkout")
                .header("x-tenant", "acme")
                .build();

        assertThat(matches(rule, traces(1), TestConsumeContext.withHeader("x-tenant", "acme"))).isTrue();
        assertThat(matches(rule, traces(1), TestConsumeContext.withHeader("x-tenant", "other"))).isFalse();
        assertThat(matches(rule, traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void domainPredicateEscapeHatchTestsTheMaterialisedDomainBatch() {
        var rule = MatchRule.builder()
                .domainPredicate(batch -> ((TracesData) batch).spanCount() == 2)
                .build();

        assertThat(matches(rule, traces(2), EMPTY_CONTEXT)).isTrue();
        assertThat(matches(rule, traces(3), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void domainPredicateForcesADomainViewRead() {
        var rule = MatchRule.builder()
                .domainPredicate(batch -> ((TracesData) batch).spanCount() == 2)
                .build();
        var tracked = new MaterialisationTrackingPayload<>(traces(2));

        assertThat(matches(rule, tracked, EMPTY_CONTEXT)).isTrue();
        assertThat(tracked.domainRequested()).isTrue();
    }

    @Test
    void resourceAttributeAndHeaderConditionsNeverRequestTheDomainView() {
        var rule = MatchRule.builder()
                .resourceAttribute("service.name", "checkout")
                .header("x-tenant", "acme")
                .build();
        var tracked = new MaterialisationTrackingPayload<>(traces(1));

        assertThat(matches(rule, tracked, TestConsumeContext.withHeader("x-tenant", "acme"))).isTrue();
        assertThat(tracked.domainRequested())
                .as("a resource-attribute and header rule must not materialise the domain")
                .isFalse();
    }

    @Test
    void aFailingResourceAttributeConditionShortCircuitsBeforeTheDomainPredicate() {
        var rule = MatchRule.builder()
                .resourceAttribute("service.name", "billing")
                .domainPredicate(batch -> {
                    throw new AssertionError("domain predicate must not run once an earlier condition fails");
                })
                .build();
        var tracked = new MaterialisationTrackingPayload<>(traces(1));

        assertThat(matches(rule, tracked, EMPTY_CONTEXT)).isFalse();
        assertThat(tracked.domainRequested()).isFalse();
    }

    @Test
    void aFailingHeaderConditionShortCircuitsBeforeTheDomainPredicate() {
        var rule = MatchRule.builder()
                .header("x-tenant", "acme")
                .domainPredicate(batch -> {
                    throw new AssertionError("domain predicate must not run once an earlier condition fails");
                })
                .build();
        var tracked = new MaterialisationTrackingPayload<>(traces(1));

        assertThat(matches(rule, tracked, EMPTY_CONTEXT)).isFalse();
        assertThat(tracked.domainRequested()).isFalse();
    }

    @Test
    void builderRejectsABlankResourceAttributeKey() {
        assertThatThrownBy(() -> MatchRule.builder().resourceAttribute(" ", "value"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void builderRejectsANullResourceAttributeValue() {
        assertThatThrownBy(() -> MatchRule.builder().resourceAttribute("key", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void builderRejectsAMalformedRegularExpression() {
        assertThatThrownBy(() -> MatchRule.builder().resourceAttributeRegex("service.name", "(unclosed"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void builderRejectsANullDomainPredicate() {
        assertThatThrownBy(() -> MatchRule.builder().domainPredicate(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /// Whether `rule` matches the single resource group of `payload`.
    private static boolean matches(MatchRule rule, Payload<?> payload, ConsumeContext context) {
        var resource = payload.routingMetadata().resources().getFirst();
        return MatchSpec.of(rule).resourceMatcher(payload, context).test(resource);
    }
}
