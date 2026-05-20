package dev.nthings.otlp4j.processor;

/// Strategy for handling a batch that arrives while the [BatchingProcessor] queue is full.
public enum DropPolicy {

    /// Drop the oldest queued batch to make room for the incoming one.
    DROP_OLDEST,

    /// Drop the incoming batch and report a partial-success rejection for its items.
    DROP_NEWEST,

    /// Block the caller's stage until the queue has capacity.
    BLOCK,

    /// Reject the incoming batch and return [dev.nthings.otlp4j.pipeline.ConsumeResult.Rejected].
    ERROR
}
