/// Signal identity and the telemetry payload marker. `SignalDescriptor` is the core-side key for one
/// of the four OTLP signals, mirroring the batch-level merge, item-count, and empty-batch operations
/// of the model `Signal` by delegating to it so the two never diverge. `SignalPair` is a connector's
/// input-to-output signal pairing. `TelemetryPayload` is the minimal marker the graph carries, kept
/// deliberately narrow so a richer payload type can replace the interim carrier without touching any
/// graph contract.
@NullMarked
package dev.nthings.otlp4j.core.signal;

import org.jspecify.annotations.NullMarked;
