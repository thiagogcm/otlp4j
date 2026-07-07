package dev.nthings.otlp4j.components.routing;

import static dev.nthings.otlp4j.components.support.BatchSupport.baseContext;
import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.internal.MatchRule;
import dev.nthings.otlp4j.components.internal.MatchSpec;
import dev.nthings.otlp4j.components.support.MaterialisationTrackingPayload;
import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.TracesData;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class FilterProcessorTest {

    private static final TestConsumeContext CTX = new TestConsumeContext();

    private final RecordingConsumer next = new RecordingConsumer();

    @Test
    void declaresNonMutatingCapabilitiesAndCompletesItsLifecycle() {
        var processor = filterProcessor(MatchSpec.none(), MatchSpec.none());

        assertThat(processor.capabilities()).isEqualTo(ConsumerCapabilities.nonMutating());
        processor.start(baseContext()).toCompletableFuture().join();
        processor.shutdown(new ShutdownContext(Duration.ofSeconds(1))).toCompletableFuture().join();
    }

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
        var processor = filterProcessor(MatchSpec.none(), exclude);

        var result = processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).isEmpty();
    }

    @Test
    void emptyIncludeAdmitsEveryPayload() {
        var processor = filterProcessor(MatchSpec.none(), MatchSpec.none());

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
        var tracked = new MaterialisationTrackingPayload(traces(1));

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
        var processor = filterProcessor(MatchSpec.none(), exclude);
        var tracked = new MaterialisationTrackingPayload(traces(1));

        var result = processor.consume(CTX, tracked).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).isEmpty();
        assertThat(tracked.domainRequested()).isTrue();
    }

    private FilterProcessor filterProcessor(MatchSpec include, MatchSpec exclude) {
        return new FilterProcessor(ComponentId.parse("filter"), SignalDescriptor.TRACES, include, exclude, next);
    }
}
