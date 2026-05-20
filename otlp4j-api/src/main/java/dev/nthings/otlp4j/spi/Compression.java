package dev.nthings.otlp4j.spi;

/// Body compression algorithm for an OTLP transport.
public enum Compression {

    /// No compression.
    NONE,

    /// gzip compression.
    GZIP
}
