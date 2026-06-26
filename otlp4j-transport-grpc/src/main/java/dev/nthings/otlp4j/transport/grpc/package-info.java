/// OTLP/gRPC transport: `OtlpGrpcExporter` and `OtlpGrpcReceiver`.
///
/// Built on gRPC + Netty; the only module that pulls in Netty so an HTTP-only deployment carries
/// no Netty dependency. Configure via [dev.nthings.otlp4j.config.ClientConfig] (exporter) or
/// [dev.nthings.otlp4j.config.ServerConfig] (receiver).
@NullMarked
package dev.nthings.otlp4j.transport.grpc;

import org.jspecify.annotations.NullMarked;
