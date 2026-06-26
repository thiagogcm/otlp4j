/// OTLP/HTTP transport: `OtlpHttpExporter` and `OtlpHttpReceiver`.
///
/// Built on `java.net.http` and `jdk.httpserver`; no gRPC or Netty dependency. Configure via
/// [dev.nthings.otlp4j.config.ClientConfig] (exporter) or
/// [dev.nthings.otlp4j.config.ServerConfig] (receiver).
@NullMarked
package dev.nthings.otlp4j.transport.http;

import org.jspecify.annotations.NullMarked;
