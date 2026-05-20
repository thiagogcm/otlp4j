package dev.nthings.otlp4j.receiver;

/// Strategy for handling tap subscribers that cannot keep up with the receiver.
///
/// A tap subscriber's lag never blocks the in-pipeline path; this enum controls what happens
/// to the tap's own buffer when it fills.
public enum BackpressureStrategy {

    /// Drop the oldest queued batch.
    DROP_OLDEST,

    /// Drop the incoming batch and increment the tap's drop counter.
    DROP_NEWEST,

    /// Block the receiver thread until the subscriber catches up. Useful for tests; risky in
    /// production because slow taps then back-pressure the in-pipeline path.
    BLOCK,

    /// Cancel the subscription by completing it exceptionally.
    ERROR
}
