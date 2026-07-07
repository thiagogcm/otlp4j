/// Receiver-runtime internals: the live telemetry tap side-channel.
///
/// [TelemetryTap] and [TapOptions] are not part of the graph runtime's public contract surface in
/// `otlp4j-core`; they are transport-runtime internals that a receiver implementation may expose for
/// side-channel live observation of accepted batches, independent of the in-pipeline acknowledgement
/// path.
@NullMarked
package dev.nthings.otlp4j.receiver;

import org.jspecify.annotations.NullMarked;
