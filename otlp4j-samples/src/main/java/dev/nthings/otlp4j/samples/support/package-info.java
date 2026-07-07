/// Demo seed components for the graph runtime: a [dev.nthings.otlp4j.samples.support.FunctionProcessorFactory]
/// that applies a pure batch transform, and a [dev.nthings.otlp4j.samples.support.CountConnectorFactory]
/// that derives a count metric from a traces batch. They stand in for the full built-in batch
/// processor and deriving connector, which are later-phase components, so the end-to-end demo and the
/// graph tests can wire a processor and a connector today.
@NullMarked
package dev.nthings.otlp4j.samples.support;

import org.jspecify.annotations.NullMarked;
