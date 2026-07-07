/// Internal implementation of the consolidated OTLP transport: the Armeria-backed
/// [dev.nthings.otlp4j.spi.OtlpServer] and [dev.nthings.otlp4j.spi.OtlpClient], the gRPC service
/// adapters, the HTTP request and response mapping, and the TLS mapping. Not exported; Armeria and
/// Netty types stay confined here so they never appear on the module's public signatures.
@NullMarked
package dev.nthings.otlp4j.transport.otlp.internal;

import org.jspecify.annotations.NullMarked;
