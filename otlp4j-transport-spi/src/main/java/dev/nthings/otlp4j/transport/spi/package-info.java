/// Shared implementation machinery for the bundled OTLP transports: the abstract exporter/receiver
/// bases they subclass, plus the receiver-side dispatch and tap plumbing. Internal — not part of the
/// supported otlp4j API.
///
/// A custom wire transport implements the `dev.nthings.otlp4j.spi` contracts (`OtlpClient` /
/// `OtlpServer`); a custom terminal implements `dev.nthings.otlp4j.exporter.Exporter`. Neither path
/// subclasses anything here.
@NullMarked
package dev.nthings.otlp4j.transport.spi;

import org.jspecify.annotations.NullMarked;
