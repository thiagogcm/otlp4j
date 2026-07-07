package dev.nthings.otlp4j.config;

/// Body compression algorithm for an OTLP transport.
///
/// This is a client-side request knob. The server side is decode-only: gRPC and HTTP receivers
/// transparently decompress gzip request bodies and do not expose a compression switch.
/// Response compression is intentionally absent because OTLP export responses are negligible.
public enum Compression {

    /// No compression.
    NONE,

    /// gzip compression.
    GZIP
}
