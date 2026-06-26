/// Ingest endpoints: receive OTLP telemetry and expose it to the pipeline.
///
/// `OtlpGrpcReceiver` serves OTLP/gRPC and exposes one `Source` per signal plus a `TelemetryTap`
/// for demand-aware live observation. Attach a single consumer per signal through the builder
/// (`onTraces`, `onMetrics`, …) or wire richer graphs with `Pipeline.from(receiver.traces())`.
@NullMarked
package dev.nthings.otlp4j.receiver;

import org.jspecify.annotations.NullMarked;
