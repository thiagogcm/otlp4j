package dev.nthings.otlp4j.pipeline;

/// A stage that wraps a downstream [TelemetryConsumer] with a telemetry transform.
///
/// Processors compose with [#andThen(Processor)] and can be assembled into a [Pipeline].
@FunctionalInterface
public interface Processor {

    /// Wraps `next`, returning a consumer that applies this processor's transform first.
    TelemetryConsumer apply(TelemetryConsumer next);

    /// Returns a processor that applies this one, then `after`, then the downstream consumer.
    default Processor andThen(Processor after) {
        return next -> this.apply(after.apply(next));
    }

    /// The identity processor: forwards every signal unchanged.
    static Processor identity() {
        return next -> next;
    }
}
