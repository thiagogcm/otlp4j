/// The consolidated OTLP transport on Armeria: [OtlpArmeriaExporter] and [OtlpArmeriaReceiver].
///
/// One Armeria plus Netty runtime serves and speaks both OTLP/gRPC and OTLP/HTTP behind the transport
/// SPI, replacing the two separate legacy transports. The receiver can serve either protocol or both
/// from one port; the exporter speaks one protocol per instance. Configure the receiver via
/// `ServerConfig` (or the neutral `OtlpReceiverConfig` through the factories) and the exporter via
/// `ClientConfig` (or `OtlpExporterConfig`).
@NullMarked
package dev.nthings.otlp4j.transport.otlp;

import org.jspecify.annotations.NullMarked;
