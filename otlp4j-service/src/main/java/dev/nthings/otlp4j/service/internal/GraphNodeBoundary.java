package dev.nthings.otlp4j.service.internal;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.Payload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// The single place a component invocation is normalized into a [DeliveryResult]. Wrapping every
/// consume call so a thrown exception, a null returned stage, or a failed stage becomes a delivery
/// result carrying the node's [Provenance] gives every graph edge the same failure semantics.
/// The normalization matches the framework-wide
/// convention `DeliveryResult.permanentFrom` codifies: a `CompletionException` is unwrapped, an
/// `Error` is rethrown, and an interrupt is restored.
public final class GraphNodeBoundary {

    private GraphNodeBoundary() {}

    /// Delivers `data` to `consumer`, guarding the call so any failure becomes a permanent rejection
    /// carrying `provenance` rather than escaping.
    ///
    /// @param consumer   the consumer to invoke
    /// @param provenance the provenance to stamp on a normalized failure
    /// @param context    the per-invocation delivery context
    /// @param data       the payload to deliver
    /// @param <T>        the signal domain type
    /// @return a stage completing with the delivery result; fatal errors remain exceptional
    public static <T> CompletionStage<DeliveryResult> deliver(
            SignalConsumer<T> consumer, Provenance provenance, ConsumeContext context, Payload<T> data) {
        CompletionStage<DeliveryResult> stage;
        try {
            stage = consumer.consume(context, data);
        } catch (Throwable failure) {
            return CompletableFuture.completedFuture(
                    DeliveryResult.permanentFrom("consume threw", failure, provenance));
        }
        if (stage == null) {
            return CompletableFuture.completedFuture(DeliveryResult.permanent(
                    "consumer " + provenance.componentId() + " returned a null completion stage", provenance));
        }
        return stage.handle((result, failure) -> {
            if (failure != null) {
                return DeliveryResult.permanentFrom("consume failed", failure, provenance);
            }
            if (result == null) {
                return DeliveryResult.permanent(
                        "consumer " + provenance.componentId() + " completed with a null result", provenance);
            }
            return result;
        });
    }
}
