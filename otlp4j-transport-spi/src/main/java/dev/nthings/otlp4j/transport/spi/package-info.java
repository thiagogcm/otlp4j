/// Transport SPI shared by the bundled OTLP transports. [ClientExporter] and
/// [ServerReceiver] are composition primitives that entry points build on, plus
/// package-private dispatch and tap plumbing. Most callers use the `OtlpGrpc*` /
/// `OtlpHttp*` entry points instead.
///
/// [ClientExporter] adapts any `OtlpClient` into an `OtlpExporter`; [ServerReceiver]
/// adapts any `OtlpServer` into a `Receiver`, so custom wire transports can reuse
/// either instead of reimplementing the lifecycle and dispatch wiring.
@NullMarked
package dev.nthings.otlp4j.transport.spi;

import org.jspecify.annotations.NullMarked;
