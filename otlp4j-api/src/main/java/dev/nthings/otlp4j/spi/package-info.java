/// The transport service-provider interface and its configuration types.
///
/// Implement `OtlpClientProvider`/`OtlpClient` (export) and `OtlpServerProvider`/`OtlpServer`
/// (receive) to supply a custom transport, discovered through `java.util.ServiceLoader`.
/// Configuration flows in through `ClientTransportConfig`/`ServerTransportConfig`, with the `Tls`,
/// `Compression`, and `RetryPolicy` selectors. These are implementation contracts — most callers
/// use `OtlpGrpcReceiver` and `OtlpGrpcExporter` instead.
package dev.nthings.otlp4j.spi;
