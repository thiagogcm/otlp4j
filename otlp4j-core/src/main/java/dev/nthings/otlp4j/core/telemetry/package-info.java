/// The gateway self-observability SPI. `GatewayMetrics` is the counter and gauge registry every
/// instrumented component reports through; `NoopGatewayMetrics` is the shared zero-cost default. A
/// recording or slf4j adapter (for example the samples module's `LoggingGatewayMetrics`) or an
/// OpenTelemetry-SDK adapter turns the instruments into real metrics.
@NullMarked
package dev.nthings.otlp4j.core.telemetry;

import org.jspecify.annotations.NullMarked;
