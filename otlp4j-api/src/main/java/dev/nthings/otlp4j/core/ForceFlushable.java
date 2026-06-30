package dev.nthings.otlp4j.core;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// A resource that can flush its buffered telemetry downstream on demand.
///
/// A pipeline forceFlush reaches every owned resource that is [ForceFlushable]; see
/// [Pipeline.Stage#owns].
public interface ForceFlushable {

    /// Flushes in-flight buffers downstream.
    ///
    /// @param timeout the flush deadline
    /// @return a stage that completes when flushed
    CompletionStage<Void> forceFlush(Duration timeout);
}
