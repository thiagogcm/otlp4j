/// The signal consumer contract and the per-invocation delivery context. `SignalConsumer` is the
/// typed consume interface, and `ConsumeContext` carries receiver-captured client metadata, the
/// deadline, and the cancellation signal that appear in every consume call. `TelemetryInput` is the
/// application-facing typed ingress for submitting domain batches into an in-process pipeline.
@NullMarked
package dev.nthings.otlp4j.core.consumer;

import org.jspecify.annotations.NullMarked;
