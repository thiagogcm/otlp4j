/// Internal implementation of the consolidated OTLP transport: the Armeria-backed wire client and
/// server ([ArmeriaOtlpClient], [ArmeriaOtlpServer]), the receiver-side per-signal routing, admission
/// control and telemetry tap, the exporter/receiver graph adapters, the gRPC service adapters, the
/// HTTP request and response mapping, the TLS mapping, and the bind-address/mTLS helpers. Not exported;
/// Armeria, Netty, and proto types stay confined here so they never appear on the module's public
/// signatures.
@NullMarked
package dev.nthings.otlp4j.transport.internal;

import org.jspecify.annotations.NullMarked;
