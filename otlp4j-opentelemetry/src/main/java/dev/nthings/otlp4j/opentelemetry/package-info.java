/// Bridges the OpenTelemetry Java SDK's push exporters to in-process otlp4j inputs.
///
/// The bridge snapshots SDK data into the immutable otlp4j model before handing it to an input.
/// It does not own, start, or close that input.
@org.jspecify.annotations.NullMarked
package dev.nthings.otlp4j.opentelemetry;
