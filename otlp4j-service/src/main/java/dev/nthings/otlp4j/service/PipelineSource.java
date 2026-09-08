package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// A component that can feed a pipeline's head, the type a [PipelineBuilder#from] argument must be. A
/// receiver is a source, and a connector is a source when it bridges into this pipeline as its
/// receiver; no other kind can. Because a [ProcessorHandle] or an [ExporterHandle] is not a source,
/// wiring one as a pipeline's head is a compile error rather than a runtime `UNKNOWN_RECEIVER_REF`
/// diagnostic.
public sealed interface PipelineSource permits ReceiverHandle, ConnectorHandle, InputHandle {

    /// The id of the component this handle refers to.
    ///
    /// @return the component id
    ComponentId id();
}
