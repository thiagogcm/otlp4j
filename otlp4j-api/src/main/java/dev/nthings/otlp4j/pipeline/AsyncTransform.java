package dev.nthings.otlp4j.pipeline;

import java.util.concurrent.CompletionStage;

/// An asynchronous 1-to-1 transformation over a signal batch.
///
/// Async transforms are the asynchronous counterpart of [Transform]: use one when a stage needs
/// I/O - an enrichment lookup, a remote sampling decision - that must not run synchronously on the
/// dispatch path. The pipeline delivers downstream only once the returned stage completes, so
/// the transport acknowledgement still covers the full chain.
///
/// The contract mirrors [Transform]: complete with a non-null batch. Drop is not signalled by
/// the async transform - complete with a pruned (possibly empty) batch and follow with
/// [Pipeline.Stage#filter]: `.transformAsync(sampler).filter(t -> t.spanCount() != 0)`. A thrown
/// exception, a failed or null stage, or a null completion value each reject the batch
/// permanently.
///
/// An async transform that holds resources may also implement [AutoCloseable] (or [Lifecycle]);
/// the pipeline auto-collects it and drains it on shutdown, before the terminal's resources.
///
/// @param <T> the OTLP signal carried by this async transform
@FunctionalInterface
public interface AsyncTransform<T> {

    /// Returns a stage completing with the non-null transformed batch.
    ///
    /// @param batch the input batch
    /// @return a stage completing with the transformed batch
    CompletionStage<T> applyAsync(T batch);
}
