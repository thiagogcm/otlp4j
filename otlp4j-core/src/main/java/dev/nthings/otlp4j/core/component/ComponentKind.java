package dev.nthings.otlp4j.core.component;

/// The four kinds of gateway component, mirroring the OpenTelemetry Collector taxonomy. A receiver
/// ingests telemetry from the wire, a processor transforms it in flight, an exporter sends it
/// onward, and a connector bridges two pipelines by consuming one signal and emitting another.
public enum ComponentKind {

    /// Ingests telemetry from a transport into one or more pipelines.
    RECEIVER,

    /// Transforms telemetry in flight within a pipeline.
    PROCESSOR,

    /// Sends telemetry onward off the gateway.
    EXPORTER,

    /// Bridges two pipelines, consuming one signal and emitting another.
    CONNECTOR
}
