/// Transport service-provider contracts.
///
/// Implement [OtlpClient] (export) and [OtlpServer] (receive) to supply a custom transport; a
/// receiver hands its server the per-signal [Dispatchers] to invoke. User-facing configuration lives
/// in `dev.nthings.otlp4j.config`; most callers use the protocol modules' `OtlpGrpcExporter` /
/// `OtlpHttpExporter` and `OtlpGrpcReceiver` / `OtlpHttpReceiver` rather than these contracts.
package dev.nthings.otlp4j.spi;
