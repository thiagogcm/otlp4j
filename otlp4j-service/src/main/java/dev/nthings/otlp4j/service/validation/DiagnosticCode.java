package dev.nthings.otlp4j.service.validation;

/// The classified reasons a gateway configuration is rejected before startup. These carry over and
/// sharpen the fluent pipeline's advisory validation kinds into a declarative set the graph checks
/// before any component is constructed. The opaque-terminal and double-attach kinds the fluent path
/// needed disappear, because a declarative graph cannot hide a resource behind a lambda or attach one
/// twice.
public enum DiagnosticCode {

    /// A pipeline names a receiver id that no registered receiver (or connector) matches.
    UNKNOWN_RECEIVER_REF,

    /// A pipeline names a processor id that no registered processor matches.
    UNKNOWN_PROCESSOR_REF,

    /// A pipeline names an exporter id that no registered exporter (or connector) matches.
    UNKNOWN_EXPORTER_REF,

    /// A pipeline routes a connector id that no registered connector matches.
    UNKNOWN_CONNECTOR_REF,

    /// The service options name an extension id that no registered extension matches.
    UNKNOWN_EXTENSION_REF,

    /// A pipeline has no receivers, no processors, and no exporters.
    EMPTY_PIPELINE,

    /// A pipeline has no receivers (and no connector routed as a receiver).
    PIPELINE_NO_RECEIVERS,

    /// A pipeline has no exporters (and no connector routed as an exporter).
    PIPELINE_NO_EXPORTERS,

    /// A receiver does not support the signal of a pipeline it feeds.
    RECEIVER_SIGNAL_UNSUPPORTED,

    /// A processor does not support the signal of a pipeline it is placed in.
    PROCESSOR_SIGNAL_UNSUPPORTED,

    /// An exporter does not support the signal of a pipeline it terminates.
    EXPORTER_SIGNAL_UNSUPPORTED,

    /// A connector does not declare support for the signal pair it is asked to bridge.
    CONNECTOR_PAIR_UNSUPPORTED,

    /// A connector is routed as an exporter in some pipeline but never as a receiver, so its emitted
    /// signal is dropped.
    CONNECTOR_USED_AS_EXPORTER_ONLY,

    /// A connector is routed as a receiver in some pipeline but never as an exporter, so it has no
    /// upstream to consume.
    CONNECTOR_USED_AS_RECEIVER_ONLY,

    /// A connector's exporting pipelines span more than one distinct input signal, or its receiving
    /// pipelines span more than one distinct output signal, so it would bridge more than one distinct
    /// signal pair. Phase 1 represents a connector as a single graph node carrying one signal pair, so
    /// it cannot faithfully attribute the several input or output signals to one shared instance.
    CONNECTOR_MULTIPLE_DISTINCT_PAIRS,

    /// The same processor id appears more than once in one pipeline's processor list.
    DUPLICATE_PROCESSOR_IN_PIPELINE,

    /// A connector edge would close a cycle in the pipeline graph.
    CYCLE_DETECTED,

    /// A component is configured but is not on any path from a receiver to an exporter.
    ORPHANED_COMPONENT
}
