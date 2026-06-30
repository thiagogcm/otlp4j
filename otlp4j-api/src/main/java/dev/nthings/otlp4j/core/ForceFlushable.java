package dev.nthings.otlp4j.core;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// A resource that can flush its buffered telemetry downstream on demand.
///
/// A pipeline `forceFlush` reaches every owned resource that is `ForceFlushable`; see
/// `Pipeline.Stage.owns(AutoCloseable)`.
public interface ForceFlushable {

    /// Flushes in-flight buffers downstream, bounded by `timeout`.
    CompletionStage<Void> forceFlush(Duration timeout);
}
