package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// Shared failure-normalization for [Processor] attachment boundaries (mid-chain [Pipeline.Stage#via]
/// and bound [Processor#asSink]). One copy of the try / requireNonNull / exceptionally-to-permanent
/// mapping, identical in shape to [Pipeline]'s terminal `deliver`.
final class Processing {
    private Processing() {}

    static <T> CompletionStage<ConsumeResult> run(Processor<T> p, T batch, Sink<? super T> next) {
        CompletionStage<ConsumeResult> stage;
        try {
            stage = Objects.requireNonNull(p.process(batch, next), "processor returned a null stage");
        } catch (Throwable e) {
            return CompletableFuture.completedFuture(ConsumeResult.permanentFrom("pipeline processor threw", e));
        }
        return stage.exceptionally(t -> ConsumeResult.permanentFrom("pipeline processor failed", t));
    }
}
