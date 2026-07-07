/// The otlp4j-core namespace root. The public contract surface lives in the exported subpackages:
/// `component` for component identity, kind, lifecycle, and factories; `signal` for signal
/// descriptors and the telemetry payload marker; `consumer` for the signal consumer contract and
/// delivery context; `result` for the delivery result taxonomy; `pipeline` for pipeline identity;
/// and `policy` for the neutral policy vocabulary. The relocated self-observability SPI keeps its
/// original `dev.nthings.otlp4j.telemetry` package name.
///
/// This root package itself carries no exported type; it exists only to document the namespace.
@NullMarked
package dev.nthings.otlp4j.core;

import org.jspecify.annotations.NullMarked;
