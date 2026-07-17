/// The consolidated OTLP transport on Armeria.
///
/// [OtlpExporter] and [OtlpReceiver] are the standalone entry points; [OtlpExporterFactory] and
/// [OtlpReceiverFactory] are the graph component factories, each protocol-parameterized through
/// `grpc()` and `http()`. One Armeria plus Netty runtime serves and speaks both OTLP/gRPC and
/// OTLP/HTTP: the receiver can serve either protocol or both from one port, and the exporter speaks one
/// protocol per instance. Configure the exporter via [OtlpExporterConfig] and the receiver via
/// [OtlpReceiverConfig], selecting [Tls] and [Compression]. [ClientExporter] and [ServerReceiver] are
/// the built runtimes the entry points return; the Armeria-backed internals live in the unexported
/// `internal` package.
@NullMarked
package dev.nthings.otlp4j.transport;

import org.jspecify.annotations.NullMarked;
