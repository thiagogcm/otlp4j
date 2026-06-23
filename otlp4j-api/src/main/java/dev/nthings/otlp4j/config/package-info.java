/// User-facing exporter and receiver configuration.
///
/// [ClientConfig] and [ServerConfig] carry the transport settings an exporter and receiver are built
/// with, selected through [Tls], [Compression], and [RetryPolicy]. These are the configuration types
/// callers touch directly; the transport contracts live in `dev.nthings.otlp4j.spi`.
package dev.nthings.otlp4j.config;
