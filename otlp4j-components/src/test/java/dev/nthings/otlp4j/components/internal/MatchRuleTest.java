package dev.nthings.otlp4j.components.internal;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.components.support.MaterialisationTrackingPayload;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.config.ConfigNode;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.model.TracesData;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/// Proves the shared match engine's conjunction semantics over resource attributes, headers, and the
/// domain-predicate escape hatch, and that only the escape hatch forces domain materialisation.
class MatchRuleTest {

    private static final TestConsumeContext EMPTY_CONTEXT = new TestConsumeContext();

    @Test
    void matchAllMatchesEveryPayloadRegardlessOfContext() {
        var rule = MatchRule.matchAll();
        assertThat(rule.matches(traces(1), EMPTY_CONTEXT)).isTrue();
        assertThat(rule.matches(traces(1), TestConsumeContext.withHeader("x", "y"))).isTrue();
    }

    @Test
    void resourceAttributeExactMatch() {
        var rule = MatchRule.builder().resourceAttribute("service.name", "checkout").build();
        assertThat(rule.matches(traces(1), EMPTY_CONTEXT)).isTrue();
    }

    @Test
    void resourceAttributeExactMismatchFails() {
        var rule = MatchRule.builder().resourceAttribute("service.name", "billing").build();
        assertThat(rule.matches(traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void resourceAttributeAbsentKeyFails() {
        var rule = MatchRule.builder().resourceAttribute("does.not.exist", "anything").build();
        assertThat(rule.matches(traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void resourceAttributeRegexMatch() {
        var matching = MatchRule.builder().resourceAttributeRegex("service.name", "check.*").build();
        var mismatching = MatchRule.builder().resourceAttributeRegex("service.name", "^other$").build();

        assertThat(matching.matches(traces(1), EMPTY_CONTEXT)).isTrue();
        assertThat(mismatching.matches(traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void resourceAttributeRegexIsAFullMatchNotASubstringSearch() {
        // "check" alone would match a substring search but must not satisfy a full match against
        // "checkout".
        var rule = MatchRule.builder().resourceAttributeRegex("service.name", "check").build();
        assertThat(rule.matches(traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void headerExactMatch() {
        var rule = MatchRule.builder().header("x-tenant", "acme").build();

        assertThat(rule.matches(traces(1), TestConsumeContext.withHeader("x-tenant", "acme"))).isTrue();
        assertThat(rule.matches(traces(1), TestConsumeContext.withHeader("x-tenant", "other"))).isFalse();
        assertThat(rule.matches(traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void headerMatchesWhenAnyOfSeveralValuesSatisfiesTheCondition() {
        var rule = MatchRule.builder().header("x-tenant", "b").build();
        var context = TestConsumeContext.withHeader("x-tenant", "a", "b", "c");
        assertThat(rule.matches(traces(1), context)).isTrue();
    }

    @Test
    void headerRegexMatch() {
        var rule = MatchRule.builder().headerRegex("x-tenant", "acme-.*").build();
        assertThat(rule.matches(traces(1), TestConsumeContext.withHeader("x-tenant", "acme-prod"))).isTrue();
        assertThat(rule.matches(traces(1), TestConsumeContext.withHeader("x-tenant", "other"))).isFalse();
    }

    @Test
    void conjunctionRequiresEveryConfiguredConditionToHold() {
        var rule = MatchRule.builder()
                .resourceAttribute("service.name", "checkout")
                .header("x-tenant", "acme")
                .build();

        assertThat(rule.matches(traces(1), TestConsumeContext.withHeader("x-tenant", "acme"))).isTrue();
        assertThat(rule.matches(traces(1), TestConsumeContext.withHeader("x-tenant", "other"))).isFalse();
        assertThat(rule.matches(traces(1), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void domainPredicateEscapeHatchTestsTheMaterialisedDomainBatch() {
        var rule = MatchRule.builder()
                .domainPredicate(batch -> ((TracesData) batch).spanCount() == 2)
                .build();

        assertThat(rule.matches(traces(2), EMPTY_CONTEXT)).isTrue();
        assertThat(rule.matches(traces(3), EMPTY_CONTEXT)).isFalse();
    }

    @Test
    void domainPredicateForcesADomainViewRead() {
        var rule = MatchRule.builder()
                .domainPredicate(batch -> ((TracesData) batch).spanCount() == 2)
                .build();
        var tracked = new MaterialisationTrackingPayload(traces(2));

        assertThat(rule.matches(tracked, EMPTY_CONTEXT)).isTrue();
        assertThat(tracked.domainRequested()).isTrue();
    }

    @Test
    void resourceAttributeAndHeaderConditionsNeverRequestTheDomainView() {
        var rule = MatchRule.builder()
                .resourceAttribute("service.name", "checkout")
                .header("x-tenant", "acme")
                .build();
        var tracked = new MaterialisationTrackingPayload(traces(1));

        assertThat(rule.matches(tracked, TestConsumeContext.withHeader("x-tenant", "acme"))).isTrue();
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
        var tracked = new MaterialisationTrackingPayload(traces(1));

        assertThat(rule.matches(tracked, EMPTY_CONTEXT)).isFalse();
        assertThat(tracked.domainRequested()).isFalse();
    }

    @Test
    void fromConfigCombinesAllFourConditionMapsIntoOneRule() {
        var node = ConfigNode.of(Map.of(
                "resource_attribute", Map.of("service.name", "checkout"),
                "resource_attribute_regex", Map.of("service.name", "check.*"),
                "header", Map.of("x-tenant", "acme"),
                "header_regex", Map.of("x-region", "us-.*")));

        var rule = MatchRule.fromConfig(node).orElseThrow();

        var bothHeaders = new TestConsumeContext(new ClientMetadata(
                Optional.empty(), Map.of("x-tenant", List.of("acme"), "x-region", List.of("us-east"))));
        assertThat(rule.matches(traces(1), bothHeaders)).isTrue();
        // Break just the header_regex condition: the conjunction must then fail.
        assertThat(rule.matches(traces(1), TestConsumeContext.withHeader("x-tenant", "acme"))).isFalse();
        assertThat(node.unreadKeys()).isEmpty();
    }

    @Test
    void fromConfigYieldsNoRuleWhenNoConditionMapIsPresent() {
        assertThat(MatchRule.fromConfig(ConfigNode.empty())).isEmpty();
        assertThat(MatchRule.fromConfig(ConfigNode.of(Map.of("unrelated", "value")))).isEmpty();
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
}
