package dev.nthings.otlp4j.core.component;

/// The five kinds of gateway component, mirroring the OpenTelemetry Collector taxonomy. A receiver
/// ingests telemetry from the wire, a processor transforms it in flight, an exporter sends it
/// onward, a connector bridges two pipelines by consuming one signal and emitting another, and an
/// extension provides a cross-cutting capability with no data path of its own.
public enum ComponentKind {

    /// Ingests telemetry from a transport into one or more pipelines.
    RECEIVER,

    /// Transforms telemetry in flight within a pipeline.
    PROCESSOR,

    /// Sends telemetry onward off the gateway.
    EXPORTER,

    /// Bridges two pipelines, consuming one signal and emitting another.
    CONNECTOR,

    /// Provides a cross-cutting capability with no telemetry data path.
    EXTENSION
}
