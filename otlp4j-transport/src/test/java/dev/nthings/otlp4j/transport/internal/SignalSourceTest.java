package dev.nthings.otlp4j.transport.internal;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("SignalSource")
class SignalSourceTest {

    private static final ComponentId RECEIVER = ComponentId.parse("otlp/in");

    private static SignalSource<TracesData> source() {
        return new SignalSource<>(TRACES, RECEIVER);
    }

    private static Payload<TracesData> payload() {
        return Payload.of(TRACES, Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER)));
    }

    private static DeliveryResult dispatch(SignalSource<TracesData> source) {
        return source.dispatch(new TestConsumeContext(), payload()).toCompletableFuture().join();
    }

    @DisplayName("dispatch returns a retryable NO_CONSUMER rejection when no consumer is attached")
    @Test
    void dispatchWithoutConsumerReturnsNoConsumer() {
        var result = dispatch(source());
        assertThat(result).isInstanceOfSatisfying(DeliveryResult.Rejected.class, rejected -> {
            assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
            assertThat(rejected.reason()).isEqualTo(DeliveryReason.NO_CONSUMER);
            assertThat(rejected.message()).contains("no consumer attached for TracesData");
        });
    }

    @DisplayName("dispatch invokes the attached consumer once per payload")
    @Test
    void dispatchInvokesAttachedConsumer() {
        var source = source();
        var hits = new AtomicInteger();
        source.attach(accepting(hits));

        assertThat(dispatch(source)).isInstanceOf(DeliveryResult.Accepted.class);
        dispatch(source);
        assertThat(hits.get()).isEqualTo(2);
    }

    @DisplayName("attach rejects a second consumer")
    @Test
    void doubleAttachRejected() {
        var source = source();
        source.attach(accepting(new AtomicInteger()));
        assertThatThrownBy(() -> source.attach(accepting(new AtomicInteger())))
                .isInstanceOf(IllegalStateException.class);
    }

    @DisplayName("dispatch propagates a synchronous consumer throw verbatim (transport-level failure)")
    @Test
    void dispatchPropagatesConsumerException() {
        var source = source();
        source.attach(new SignalConsumer<>() {
            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<TracesData> data) {
                throw new RuntimeException("boom");
            }
        });
        assertThatThrownBy(() -> source.dispatch(new TestConsumeContext(), payload()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("boom");
    }

    @DisplayName("dispatch propagates an asynchronous consumer failure verbatim (transport-level failure)")
    @Test
    void dispatchPropagatesAsyncConsumerFailure() {
        var source = source();
        source.attach(new SignalConsumer<>() {
            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<TracesData> data) {
                return CompletableFuture.failedFuture(new IllegalStateException("async boom"));
            }
        });
        var stage = source.dispatch(new TestConsumeContext(), payload());
        assertThatThrownBy(() -> stage.toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("async boom");
    }

    @DisplayName("dispatch keeps the empty payload path exercised")
    @Test
    void emptyPayloadStillDispatches() {
        var source = source();
        var hits = new AtomicInteger();
        source.attach(accepting(hits));
        var result = source.dispatch(
                new TestConsumeContext(), Payload.of(TRACES, new TracesData(List.of())))
                .toCompletableFuture().join();
        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(hits.get()).isEqualTo(1);
    }

    private static SignalConsumer<TracesData> accepting(AtomicInteger hits) {
        var provenance = new Provenance(RECEIVER, ComponentKind.EXPORTER, TRACES);
        return new SignalConsumer<>() {
            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<TracesData> data) {
                hits.incrementAndGet();
                return DeliveryResult.acceptedStage(provenance);
            }
        };
    }
}
