package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// A component that can terminate a pipeline, the type a [PipelineBuilder#to] argument must be. An
/// exporter is a target, and a connector is a target when this pipeline feeds it as its exporter; no
/// other kind can. Because a [ReceiverHandle] or a [ProcessorHandle] is not a target, wiring one as a
/// pipeline's terminal is a compile error rather than a runtime `UNKNOWN_EXPORTER_REF` diagnostic.
public sealed interface PipelineTarget permits ExporterHandle, ConnectorHandle {

    /// The id of the component this handle refers to.
    ///
    /// @return the component id
    ComponentId id();
}
