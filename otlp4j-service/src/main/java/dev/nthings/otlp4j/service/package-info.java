/// The gateway builder and runtime: the declarative replacement for the fluent pipeline's discovered
/// lifecycle.
///
/// [dev.nthings.otlp4j.service.Gateway#builder] returns a fluent assembler that compiles to a
/// [dev.nthings.otlp4j.service.config.GatewayConfig]. A [dev.nthings.otlp4j.service.GatewayBuilder]
/// validates that config, materializes the internal service graph, detects orphans and connector
/// cycles, and instantiates the components in reverse topological order into a
/// [dev.nthings.otlp4j.service.GatewayRuntime]. The runtime starts extensions first and the data path
/// in reverse topological order, rolls back on a start failure, and shuts down in forward topological
/// order. [dev.nthings.otlp4j.service.GraphNodeBoundary] normalizes every component invocation into a
/// delivery result, [dev.nthings.otlp4j.service.ExporterMetricsHelper] counts the exporter send
/// boundary, and [dev.nthings.otlp4j.core.signal.ModelBatchPayload] is the interim carrier the graph
/// moves in this milestone.
@NullMarked
package dev.nthings.otlp4j.service;

import org.jspecify.annotations.NullMarked;
