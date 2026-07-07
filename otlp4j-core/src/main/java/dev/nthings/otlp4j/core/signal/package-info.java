/// Signal identity and the telemetry payload contract. `SignalDescriptor` is the core-side key for
/// one of the four OTLP signals: it owns the batch-level merge, item-count, and empty-batch
/// operations directly over the immutable model records, so batch policy is core behaviour rather
/// than model behaviour. `SignalPair` is a connector's input-to-output signal pairing.
/// `TelemetryPayload` is the widened marker the graph carries: signal, item count, serialized size,
/// memory mode, a lazily materialising view selector, a mutation-safe copy hook, and a direct
/// serialisation to an output stream, all without naming a proto type.
@NullMarked
package dev.nthings.otlp4j.core.signal;

import org.jspecify.annotations.NullMarked;
