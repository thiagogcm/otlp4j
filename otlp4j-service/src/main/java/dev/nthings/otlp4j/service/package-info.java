/// Typed in-process pipelines and validated multi-signal gateways with explicit lifecycle ownership.
///
/// [dev.nthings.otlp4j.service.Gateway#builder] returns a fluent assembler that compiles to a
/// [dev.nthings.otlp4j.service.config.GatewayConfig]. A [dev.nthings.otlp4j.service.GatewayBuilder]
/// validates that config, materializes the internal service graph, detects orphans and connector
/// cycles, and instantiates the components in reverse topological order into a
/// [dev.nthings.otlp4j.service.GatewayRuntime]. The runtime starts the data path in reverse
/// topological order, rolls back on a start failure, and shuts down in forward topological
/// order. The internal graph-node boundary normalizes every component invocation into a
/// delivery result. The graph moves typed [dev.nthings.otlp4j.core.signal.Payload] instances without
/// depending on a wire format. [dev.nthings.otlp4j.service.Pipeline] provides the compact embedded API;
/// both construction paths use the same graph compiler and lifecycle implementation.
@NullMarked
package dev.nthings.otlp4j.service;

import org.jspecify.annotations.NullMarked;
