package dev.nthings.otlp4j.components.routing;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.support.MaterialisationTrackingPayload;
import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class FilterProcessorTest {

    private static final TestConsumeContext CTX = new TestConsumeContext();

    private final RecordingConsumer<TracesData> next = new RecordingConsumer<>();

    @Test
    void keepsAPayloadThatMatchesIncludeAndDoesNotMatchExclude() {
        var include = MatchSpec.of(MatchRule.builder().resourceAttribute("service.name", "checkout").build());
        var processor = filterProcessor(include, MatchSpec.none());

        var result = processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).hasSize(1);
    }

    @Test
    void dropsAPayloadThatDoesNotMatchInclude() {
        var include = MatchSpec.of(MatchRule.builder().resourceAttribute("service.name", "billing").build());
        var processor = filterProcessor(include, MatchSpec.none());

        var result = processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).isEmpty();
    }

    @Test
    void dropsAPayloadThatMatchesExclude() {
        var exclude = MatchSpec.of(MatchRule.builder().resourceAttribute("service.name", "checkout").build());
        var processor = filterProcessor(MatchSpec.all(), exclude);

        var result = processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).isEmpty();
    }

    @Test
    void explicitCatchAllIncludeAdmitsEveryPayload() {
        var processor = filterProcessor(MatchSpec.all(), MatchSpec.none());

        processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(next.received()).hasSize(1);
    }

    @Test
    void keepsByHeaderCondition() {
        var include = MatchSpec.of(MatchRule.builder().header("x-tenant", "acme").build());
        var processor = filterProcessor(include, MatchSpec.none());

        processor.consume(TestConsumeContext.withHeader("x-tenant", "acme"), traces(1))
                .toCompletableFuture().join();
        processor.consume(TestConsumeContext.withHeader("x-tenant", "other"), traces(1))
                .toCompletableFuture().join();

        assertThat(next.received()).hasSize(1);
    }

    @Test
    void resourceAttributeAndHeaderRulesNeverRequestTheDomainView() {
        var include = MatchSpec.of(MatchRule.builder().resourceAttribute("service.name", "checkout").build());
        var exclude = MatchSpec.of(MatchRule.builder().header("x-drop", "yes").build());
        var processor = filterProcessor(include, exclude);
        var tracked = new MaterialisationTrackingPayload<>(traces(1));

        var result = processor.consume(CTX, tracked).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).hasSize(1);
        assertThat(tracked.domainRequested())
                .as("a resource-attribute and header filter must not materialise the domain")
                .isFalse();
    }

    @Test
    void excludeByDomainPredicateMaterialisesTheDomainAndDropsAMatchingPayload() {
        var exclude = MatchSpec.of(MatchRule.builder()
                .domainPredicate(batch -> ((TracesData) batch).spanCount() == 1)
                .build());
        var processor = filterProcessor(MatchSpec.all(), exclude);
        var tracked = new MaterialisationTrackingPayload<>(traces(1));

        var result = processor.consume(CTX, tracked).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).isEmpty();
        assertThat(tracked.domainRequested()).isTrue();
    }

    @Test
    void forwardsOnlyTheMatchingResourceGroupsOfAMixedPayload() {
        var processor = filterProcessor(service("billing"), MatchSpec.none());

        var result = processor.consume(CTX, mixed()).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).singleElement().satisfies(forwarded -> {
            assertThat(forwarded.data().resourceSpans())
                    .extracting(group -> group.resource().attributes().getString("service.name"))
                    .containsExactly("billing");
            assertThat(forwarded.data().spans()).extracting(Span::name).containsExactly("c");
            assertThat(forwarded.itemCount()).isEqualTo(1);
            assertThat(forwarded.routingMetadata().resources()).hasSize(1);
        });
    }

    @Test
    void excludeRemovesOnlyTheMatchingResourceGroupOfAMixedPayload() {
        var processor = filterProcessor(MatchSpec.all(), service("billing"));

        processor.consume(CTX, mixed()).toCompletableFuture().join();

        assertThat(next.received()).singleElement().satisfies(forwarded ->
                assertThat(forwarded.data().spans()).extracting(Span::name).containsExactly("a", "b"));
    }

    @Test
    void forwardsAMixedPayloadUntouchedWhenEveryResourceGroupIsKept() {
        var include = MatchSpec.of(
                MatchRule.builder().resourceAttributeRegex("service.name", "checkout|billing").build());
        var processor = filterProcessor(include, service("payments"));
        var tracked = new MaterialisationTrackingPayload<>(mixed());

        processor.consume(CTX, tracked).toCompletableFuture().join();

        assertThat(next.received()).containsExactly(tracked);
        assertThat(tracked.domainRequested()).isFalse();
    }

    @Test
    void dropsAMixedPayloadWithoutDecodingWhenNoResourceGroupIsKept() {
        var processor = filterProcessor(service("payments"), MatchSpec.none());
        var tracked = new MaterialisationTrackingPayload<>(mixed());

        var result = processor.consume(CTX, tracked).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).isEmpty();
        assertThat(tracked.domainRequested()).isFalse();
    }

    @Test
    void aHeaderConditionGatesEveryResourceGroupOfItsRule() {
        var include = MatchSpec.of(MatchRule.builder()
                .resourceAttribute("service.name", "billing")
                .header("x-tenant", "acme")
                .build());
        var processor = filterProcessor(include, MatchSpec.none());

        processor.consume(TestConsumeContext.withHeader("x-tenant", "other"), mixed()).toCompletableFuture().join();
        assertThat(next.received()).isEmpty();

        processor.consume(TestConsumeContext.withHeader("x-tenant", "acme"), mixed()).toCompletableFuture().join();
        assertThat(next.received()).singleElement().satisfies(forwarded ->
                assertThat(forwarded.data().spans()).extracting(Span::name).containsExactly("c"));
    }

    @Test
    void aDomainPredicateRunsOnceForAMixedPayload() {
        var calls = new AtomicInteger();
        var include = MatchSpec.of(MatchRule.builder()
                .domainPredicate(batch -> {
                    calls.incrementAndGet();
                    return ((TracesData) batch).spanCount() == 3;
                })
                .build());
        var processor = filterProcessor(include, service("billing"));

        processor.consume(CTX, mixed()).toCompletableFuture().join();

        assertThat(next.received()).singleElement().satisfies(forwarded ->
                assertThat(forwarded.data().spans()).extracting(Span::name).containsExactly("a", "b"));
        assertThat(calls).hasValue(1);
    }

    @Test
    void aPayloadWithoutResourceGroupsCannotSatisfyAResourceCondition() {
        var empty = Payload.of(SignalDescriptor.TRACES, new TracesData(List.of()));

        filterProcessor(service("checkout"), MatchSpec.none()).consume(CTX, empty).toCompletableFuture().join();
        assertThat(next.received()).isEmpty();

        filterProcessor(MatchSpec.all(), service("checkout")).consume(CTX, empty).toCompletableFuture().join();
        assertThat(next.received()).containsExactly(empty);
    }

    @Test
    void aProfilesPayloadKeepsItsDictionaryWhenAResourceGroupIsDropped() {
        var dictionary = new byte[] {1, 2, 3};
        var batch = new ProfilesData(
                List.of(profiles("checkout", "0102030405060708"), profiles("billing", "1112131415161718")),
                dictionary);
        var profilesNext = new RecordingConsumer<ProfilesData>();
        var processor = new FilterProcessor<>(
                ComponentId.parse("filter"), SignalDescriptor.PROFILES, service("billing"), MatchSpec.none(),
                profilesNext);

        processor.consume(CTX, Payload.of(SignalDescriptor.PROFILES, batch)).toCompletableFuture().join();

        assertThat(profilesNext.received()).singleElement().satisfies(forwarded -> {
            assertThat(forwarded.data().profiles())
                    .extracting(ProfilesData.Profile::profileId)
                    .containsExactly("1112131415161718");
            assertThat(forwarded.data().dictionary()).isEqualTo(dictionary);
        });
    }

    @Test
    void forwardsNothingWhenTheDecodedBatchHoldsNoneOfTheGroupsItsMetadataListed() {
        var listed = RoutingMetadata.of(List.of(attributes("checkout"), attributes("billing")));
        var decoded = new TracesData(List.of(group("checkout", "a")));
        var inconsistent = new Payload<TracesData>() {
            @Override public SignalDescriptor<TracesData> signal() { return SignalDescriptor.TRACES; }
            @Override public TracesData data() { return decoded; }
            @Override public long itemCount() { return 1; }
            @Override public RoutingMetadata routingMetadata() { return listed; }
            @Override public Payload<TracesData> withData(TracesData replacement) {
                return Payload.of(SignalDescriptor.TRACES, replacement);
            }
        };

        var result = filterProcessor(service("billing"), MatchSpec.none())
                .consume(CTX, inconsistent).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).isEmpty();
    }

    private static MatchSpec service(String name) {
        return MatchSpec.of(MatchRule.builder().resourceAttribute("service.name", name).build());
    }

    /// One payload holding a `checkout` resource with spans `a` and `b`, then a `billing` resource
    /// with span `c`.
    private static Payload<TracesData> mixed() {
        return Payload.of(SignalDescriptor.TRACES, new TracesData(List.of(
                group("checkout", "a", "b"), group("billing", "c"))));
    }

    private static TracesData.ResourceSpans group(String service, String... spanNames) {
        var spans = List.of(spanNames).stream().map(name -> Fixtures.span(name, Span.Kind.SERVER)).toList();
        return new TracesData.ResourceSpans(
                Resource.of(attributes(service)),
                service + "-url",
                List.of(new TracesData.ScopeSpans(InstrumentationScope.EMPTY, "", spans)));
    }

    private static ProfilesData.ResourceProfiles profiles(String service, String profileId) {
        return new ProfilesData.ResourceProfiles(
                Resource.of(attributes(service)),
                "",
                List.of(new ProfilesData.ScopeProfiles(
                        InstrumentationScope.EMPTY, "", List.of(Fixtures.profile(profileId)))));
    }

    private static Attributes attributes(String service) {
        return Attributes.builder().put("service.name", service).build();
    }

    private FilterProcessor<TracesData> filterProcessor(MatchSpec include, MatchSpec exclude) {
        return new FilterProcessor<>(ComponentId.parse("filter"), SignalDescriptor.TRACES, include, exclude, next);
    }
}
