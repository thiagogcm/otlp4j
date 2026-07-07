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
import java.time.Duration;
import org.junit.jupiter.api.Test;

/// Proves the content-routing story: N branch copies of the routing processor, each fed the same
/// payload by an upstream fan-out (simulated here by driving each branch's `consume` directly), send
/// each payload to exactly its matching branch, and the designated default branch keeps the remainder.
class RoutingProcessorTest {

    private static final TestConsumeContext CTX = new TestConsumeContext();

    @Test
    void declaresNonMutatingCapabilitiesAndCompletesItsLifecycle() {
        var processor = branch(MatchSpec.none(), true, new RecordingConsumer());

        assertThat(processor.capabilities()).isEqualTo(ConsumerCapabilities.nonMutating());
        processor.start(baseContext()).toCompletableFuture().join();
        processor.shutdown(new ShutdownContext(Duration.ofSeconds(1))).toCompletableFuture().join();
    }

    @Test
    void forwardsWhenTheConfiguredConditionMatches() {
        var next = new RecordingConsumer();
        var processor = branch(condition("service.name", "checkout"), false, next);

        var result = processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).hasSize(1);
    }

    @Test
    void dropsWhenTheConfiguredConditionDoesNotMatch() {
        var next = new RecordingConsumer();
        var processor = branch(condition("service.name", "billing"), false, next);

        var result = processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).isEmpty();
    }

    @Test
    void eachPayloadReachesExactlyItsMatchingBranch() {
        var checkoutNext = new RecordingConsumer();
        var billingNext = new RecordingConsumer();
        var checkoutBranch = branch(condition("service.name", "checkout"), false, checkoutNext);
        var billingBranch = branch(condition("service.name", "billing"), false, billingNext);

        // The payload's resource is "checkout" (see BatchSupport/Fixtures); an upstream fan-out would
        // hand the same payload to every branch.
        checkoutBranch.consume(CTX, traces(1)).toCompletableFuture().join();
        billingBranch.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(checkoutNext.received()).hasSize(1);
        assertThat(billingNext.received()).isEmpty();
    }

    @Test
    void defaultRouteBranchKeepsTheUnmatchedRemainder() {
        var checkoutNext = new RecordingConsumer();
        var defaultNext = new RecordingConsumer();
        var checkoutBranch = branch(condition("service.name", "billing"), false, checkoutNext);
        var defaultBranch = branch(MatchSpec.none(), true, defaultNext);

        // The payload matches neither the specific branch's condition (its resource is "checkout", not
        // "billing"), so only the default branch keeps it.
        checkoutBranch.consume(CTX, traces(1)).toCompletableFuture().join();
        defaultBranch.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(checkoutNext.received()).isEmpty();
        assertThat(defaultNext.received()).hasSize(1);
    }

    @Test
    void defaultRouteBranchAlsoForwardsAPayloadASiblingBranchAlreadyClaimed() {
        // Documented characteristic: a routing processor only ever evaluates its own configuration, so
        // the default branch forwards unconditionally regardless of what a sibling branch decided.
        var checkoutNext = new RecordingConsumer();
        var defaultNext = new RecordingConsumer();
        var checkoutBranch = branch(condition("service.name", "checkout"), false, checkoutNext);
        var defaultBranch = branch(MatchSpec.none(), true, defaultNext);

        checkoutBranch.consume(CTX, traces(1)).toCompletableFuture().join();
        defaultBranch.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(checkoutNext.received()).hasSize(1);
        assertThat(defaultNext.received()).hasSize(1);
    }

    @Test
    void defaultRouteWithANonEmptyMatchStillEvaluatesTheMatchNormally() {
        var next = new RecordingConsumer();
        // defaultRoute is set, but match is not empty, so the flag only ever changes the meaning of an
        // *empty* match; a configured match still governs normally.
        var processor = branch(condition("service.name", "billing"), true, next);

        processor.consume(CTX, traces(1)).toCompletableFuture().join();

        assertThat(next.received()).isEmpty();
    }

    @Test
    void aResourceAttributeConditionNeverRequestsTheDomainView() {
        var next = new RecordingConsumer();
        var processor = branch(condition("service.name", "checkout"), false, next);
        var tracked = new MaterialisationTrackingPayload(traces(1));

        processor.consume(CTX, tracked).toCompletableFuture().join();

        assertThat(next.received()).hasSize(1);
        assertThat(tracked.domainRequested()).isFalse();
    }

    private static MatchSpec condition(String key, String value) {
        return MatchSpec.of(MatchRule.builder().resourceAttribute(key, value).build());
    }

    private static RoutingProcessor branch(MatchSpec match, boolean defaultRoute, RecordingConsumer next) {
        return new RoutingProcessor(ComponentId.parse("routing"), SignalDescriptor.TRACES, match, defaultRoute, next);
    }
}
