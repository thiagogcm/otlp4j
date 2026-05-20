package dev.nthings.otlp4j.pipeline;

/// Describes whether a [Consumer] may mutate the batch it receives.
///
/// Used by [FanOut] to decide whether to share a single batch reference between peers
/// ([#IMMUTABLE]) or hand each peer a defensive copy ([#MUTATES_DATA]).
public enum Capabilities {

    /// The consumer does not retain or mutate the batch; the batch may be safely shared with peers.
    IMMUTABLE,

    /// The consumer may retain or mutate the batch; peers must each receive an independent copy.
    MUTATES_DATA
}
