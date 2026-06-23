package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.pipeline.Transform;
import java.util.concurrent.CompletionStage;

/// Derives telemetry of one signal `O` from telemetry of another signal `I`.
///
/// Unlike a [Transform] a connector is allowed to change the signal
/// type and to emit batches that do not correspond 1:1 with the input. The framework treats a
/// connector as a [Sink] of `I` that holds a downstream [Sink] of `O`.
///
/// The built-in count connectors expose a [FailurePolicy] controlling whether a downstream
/// delivery failure of the derived `O` is propagated onto the input result.
///
/// @param <I> the input OTLP signal
/// @param <O> the OTLP signal emitted downstream
public interface Connector<I, O> extends Sink<I> {

    /// The downstream consumer this connector emits into.
    Sink<? super O> downstream();

    @Override
    CompletionStage<ConsumeResult<I>> consume(I batch);
}
