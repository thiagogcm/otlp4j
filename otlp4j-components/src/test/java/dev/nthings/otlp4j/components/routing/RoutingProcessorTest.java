package dev.nthings.otlp4j.components.routing;

import static dev.nthings.otlp4j.components.support.BatchSupport.traces;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.support.MaterialisationTrackingPayload;
import dev.nthings.otlp4j.components.support.RecordingConsumer;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.TracesData;
import org.junit.jupiter.api.Test;

class RoutingProcessorTest {

    private static final TestConsumeContext CTX = new TestConsumeContext();

    @Test
    void emptySpecDropsAndExplicitMatchAllForwards() {
        var dropped = new RecordingConsumer<TracesData>();
        var empty = branch(MatchSpec.none(), dropped);
        assertThat(empty.consume(CTX, traces(1)).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(dropped.received()).isEmpty();

        var forwarded = new RecordingConsumer<TracesData>();
        var all = branch(MatchSpec.of(MatchRule.matchAll()), forwarded);
        all.consume(CTX, traces(1)).toCompletableFuture().join();
        assertThat(forwarded.received()).hasSize(1);
    }

    @Test
    void forwardsOnlyWhenConfiguredConditionMatches() {
        var next = new RecordingConsumer<TracesData>();
        var processor = branch(condition("service.name", "checkout"), next);
        processor.consume(CTX, traces(1)).toCompletableFuture().join();
        assertThat(next.received()).hasSize(1);
        var other = branch(condition("service.name", "billing"), next);
        other.consume(CTX, traces(1)).toCompletableFuture().join();
        assertThat(next.received()).hasSize(1);
    }

    @Test
    void resourceConditionDoesNotRequestDomain() {
        var next = new RecordingConsumer<TracesData>();
        var processor = branch(condition("service.name", "checkout"), next);
        var tracked = new MaterialisationTrackingPayload<>(traces(1));

        assertThat(processor.consume(CTX, tracked).toCompletableFuture().join())
                .isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(next.received()).containsExactly(tracked);

        assertThat(tracked.domainRequested()).isFalse();
    }

    private static MatchSpec condition(String key, String value) {
        return MatchSpec.of(MatchRule.builder().resourceAttribute(key, value).build());
    }

    private static RoutingProcessor<TracesData> branch(
            MatchSpec match, RecordingConsumer<TracesData> next) {
        return new RoutingProcessor<>(ComponentId.parse("routing"), SignalDescriptor.TRACES, match, next);
    }
}
