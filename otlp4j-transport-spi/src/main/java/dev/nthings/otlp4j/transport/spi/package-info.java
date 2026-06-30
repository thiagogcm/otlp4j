/// The transport SPI shared by the bundled OTLP transports: the [ClientExporter] / [ServerReceiver]
/// composition primitives their entry points build on, plus the package-private dispatch and tap
/// plumbing. Most callers use the `OtlpGrpc*` / `OtlpHttp*` entry points instead.
///
/// [ClientExporter] adapts any `OtlpClient` into an `OtlpExporter`; [ServerReceiver] adapts any
/// `OtlpServer` into a `Receiver`, so a custom wire transport can reuse either instead of
/// reimplementing the lifecycle and dispatch wiring.
@NullMarked
package dev.nthings.otlp4j.transport.spi;

import org.jspecify.annotations.NullMarked;
