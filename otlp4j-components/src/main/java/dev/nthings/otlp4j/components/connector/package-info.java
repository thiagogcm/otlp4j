/// The built-in span-count connector: a [dev.nthings.otlp4j.components.connector.SpanCountConnectorFactory]
/// that derives a single delta-sum count metric from each traces batch and emits it into the receiving
/// metrics pipeline. It is the connector member of the built-in component set, composing a traces
/// pipeline with a metrics pipeline, which is the one thing a connector does that a terminal-then-source
/// pair cannot.
@NullMarked
package dev.nthings.otlp4j.components.connector;

import org.jspecify.annotations.NullMarked;
