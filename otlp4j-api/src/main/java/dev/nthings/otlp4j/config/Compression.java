package dev.nthings.otlp4j.config;

/// Body compression algorithm for an OTLP transport.
///
/// This is a client-side request knob. The server side is decode-only: an OTLP receiver
/// transparently decompresses gzip request bodies via gRPC's default decoder and does not expose a
/// compression switch — see [ServerConfig] for why response compression is intentionally
/// not configured.
public enum Compression {

    /// No compression.
    NONE,

    /// gzip compression.
    GZIP
}
