/// The gateway self-observability SPI, relocated from otlp4j-api with its package name unchanged so
/// no importing component or transport changed a single import. `GatewayMetrics` is the counter and
/// gauge registry every instrumented component reports through; `NoopGatewayMetrics` is the shared
/// zero-cost default and `LoggingGatewayMetrics` is the dependency-free slf4j reference adapter.
@NullMarked
package dev.nthings.otlp4j.telemetry;

import org.jspecify.annotations.NullMarked;
