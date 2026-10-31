package dev.nthings.otlp4j.service;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryReason;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.UndecodablePayloadException;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.internal.GraphNodeBoundary;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

/// Unit tests for the graph's delivery boundary normalization.
class HelpersTest {

    private static final Provenance PROV =
            new Provenance(ComponentId.parse("otlpexp"), ComponentKind.EXPORTER, TRACES);

    private final ConsumeContext ctx = new TestConsumeContext();

    private final Payload<TracesData> payload = Payload.of(TRACES, new TracesData(List.of()));

    private static SignalConsumer<TracesData> consumer(
            BiFunction<ConsumeContext, Payload<TracesData>, CompletionStage<DeliveryResult>> body) {
        return new SignalConsumer<>() {
            @Override
            public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<TracesData> data) {
                return body.apply(context, data);
            }
        };
    }

    @Test
    void boundaryPassesThroughSuccess() {
        var result = GraphNodeBoundary.deliver(
                        consumer((c, d) -> DeliveryResult.acceptedStage(PROV)), PROV, ctx, payload)
                .toCompletableFuture()
                .join();
        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
    }

    @Test
    void boundaryNormalizesSynchronousThrow() {
        var result = GraphNodeBoundary.deliver(
                        consumer((c, d) -> {
                            throw new IllegalStateException("boom");
                        }),
                        PROV,
                        ctx,
                        payload)
                .toCompletableFuture()
                .join();
        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        var rejected = (DeliveryResult.Rejected) result;
        // A thrown exception nobody classified is retryable, so an upstream sender does not drop data
        // over what may be a transient fault.
        assertThat(rejected.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(rejected.reason()).isEqualTo(DeliveryReason.LOCAL_FAILURE);
        assertThat(rejected.provenance()).isEqualTo(PROV);
    }

    @Test
    void boundaryNormalizesNullStage() {
        var result = GraphNodeBoundary.deliver(consumer((c, d) -> null), PROV, ctx, payload)
                .toCompletableFuture()
                .join();
        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        // A null stage is a component defect, not a transient fault.
        assertThat(((DeliveryResult.Rejected) result).disposition()).isEqualTo(Disposition.PERMANENT);
    }

    @Test
    void boundaryNormalizesFailedStage() {
        var result = GraphNodeBoundary.deliver(
                        consumer((c, d) -> CompletableFuture.failedFuture(new IllegalStateException("late"))),
                        PROV,
                        ctx,
                        payload)
                .toCompletableFuture()
                .join();
        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) result).disposition()).isEqualTo(Disposition.RETRYABLE);
    }

    @Test
    void boundaryTreatsUndecodablePayloadAsPermanent() {
        var result = (DeliveryResult.Rejected) GraphNodeBoundary.deliver(
                        consumer((c, d) -> CompletableFuture.failedFuture(
                                new UndecodablePayloadException("bad", new IllegalStateException()))),
                        PROV,
                        ctx,
                        payload)
                .toCompletableFuture()
                .join();
        assertThat(result.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(result.reason()).isEqualTo(DeliveryReason.PROTOCOL_TRANSLATION);
    }

    @Test
    void boundaryTreatsTimeoutAsRetryableTimeout() {
        var result = (DeliveryResult.Rejected) GraphNodeBoundary.deliver(
                        consumer((c, d) -> CompletableFuture.failedFuture(new TimeoutException("slow"))),
                        PROV,
                        ctx,
                        payload)
                .toCompletableFuture()
                .join();
        assertThat(result.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(result.reason()).isEqualTo(DeliveryReason.TIMEOUT);
    }

    @Test
    void boundaryNormalizesNullCompletedResult() {
        var result = GraphNodeBoundary.deliver(
                        consumer((c, d) -> CompletableFuture.completedFuture(null)), PROV, ctx, payload)
                .toCompletableFuture()
                .join();
        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) result).disposition()).isEqualTo(Disposition.PERMANENT);
    }
}
