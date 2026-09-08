/// Signal identity and the telemetry payload contract. `SignalDescriptor` is the core-side key for
/// one of the four OTLP signals: it owns the batch-level merge, item-count, and empty-batch
/// operations directly over the immutable model records, so batch policy is core behaviour rather
/// than model behaviour. `SignalPair` is a connector's input-to-output signal pairing.
/// [Payload] is the immutable, typed domain value the graph carries. Wire-specific encoding belongs
/// to the codec module and is not part of this core contract.
@NullMarked
package dev.nthings.otlp4j.core.signal;

import org.jspecify.annotations.NullMarked;
