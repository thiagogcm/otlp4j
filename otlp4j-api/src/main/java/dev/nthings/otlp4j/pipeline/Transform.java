package dev.nthings.otlp4j.pipeline;

/// A pure 1→1 transformation over a signal batch.
///
/// Transforms are the building block of `Pipeline.transform(...)` and the most common pipeline
/// component. They are stateless from the framework's point of view; implementations are free to
/// hold immutable configuration but must not retain or mutate batches.
///
/// @param <T> the OTLP signal carried by this transform
@FunctionalInterface
public interface Transform<T> {

    /// Returns a new batch derived from `batch`. May return `batch` unchanged.
    T apply(T batch);
}
