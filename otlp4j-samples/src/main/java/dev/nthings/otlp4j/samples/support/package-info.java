/// Demo seed components for the graph runtime: a [dev.nthings.otlp4j.samples.support.CountConnectorFactory]
/// that derives a count metric from a traces batch. It stands in for the full built-in deriving
/// connector, a later-phase component, so the end-to-end demo and the graph tests can wire a connector
/// today; the demo's per-signal transforms are built inline through
/// [dev.nthings.otlp4j.core.component.ProcessorFactory#of].
@NullMarked
package dev.nthings.otlp4j.samples.support;

import org.jspecify.annotations.NullMarked;
