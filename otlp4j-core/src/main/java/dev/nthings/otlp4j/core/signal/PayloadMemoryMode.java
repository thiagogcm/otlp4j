package dev.nthings.otlp4j.core.signal;

/// The authoritative, already-in-hand representation of a [TelemetryPayload], which the exporter and
/// the fan-out node read to reason about cost without downcasting. It tells the reader which direction
/// is free and which costs a materialisation.
///
/// [#PROTO_BACKED] names a payload whose in-hand representation is the proto export request, so its
/// `DOMAIN_IMMUTABLE` view is the one that costs a materialisation; this is a receiver-decoded
/// pass-through payload. [#DOMAIN_BACKED] names a payload whose in-hand representation is the domain
/// batch, so its `PROTO_REQUEST` view is the one that costs a materialisation; this is a
/// processor-produced payload.
///
/// In Phase 2 both modes are immutable and share-safe, so the mode is advisory for cost, not for
/// safety, until a mutable mode ships.
public enum PayloadMemoryMode {

    /// The proto export request is the in-hand representation; the domain view costs a materialisation.
    PROTO_BACKED,

    /// The domain batch is the in-hand representation; the proto view costs a materialisation.
    DOMAIN_BACKED
}
