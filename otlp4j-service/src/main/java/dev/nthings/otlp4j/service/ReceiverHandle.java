package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// The typed handle a [GatewayBuilder#receiver] registration returns: a thin value carrying the
/// registered receiver's [ComponentId], with the component kind captured in its type so a pipeline can
/// reference it only where a receiver is allowed (a [PipelineSource]). Passing it anywhere a receiver
/// does not belong is a compile error.
///
/// @param id the id the receiver was registered under
public record ReceiverHandle(ComponentId id) implements PipelineSource {

    /// Validates that the id is present.
    public ReceiverHandle {
        if (id == null) {
            throw new IllegalArgumentException("receiver handle id must be non-null");
        }
    }
}
