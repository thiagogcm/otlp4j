package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.pipeline.Consumer;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import java.util.concurrent.CompletionStage;

/// Derives telemetry of one signal `O` from telemetry of another signal `I`.
///
/// Unlike a [dev.nthings.otlp4j.pipeline.Transform] a connector is allowed to change the signal
/// type and to emit batches that do not correspond 1:1 with the input. The framework treats a
/// connector as a [Consumer] of `I` that holds a downstream [Consumer] of `O`.
///
/// @param <I> the input OTLP signal
/// @param <O> the OTLP signal emitted downstream
public interface Connector<I, O> extends Consumer<I> {

    /// The downstream consumer this connector emits into.
    Consumer<? super O> downstream();

    @Override
    CompletionStage<ConsumeResult<I>> consume(I batch);
}
