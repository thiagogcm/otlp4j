package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// The typed handle a [GatewayBuilder#exporter] registration returns: a thin value carrying the
/// registered exporter's [ComponentId], with the component kind captured in its type so a pipeline can
/// reference it only where an exporter is allowed (a [PipelineTarget]). Passing it anywhere an
/// exporter does not belong is a compile error.
///
/// @param id the id the exporter was registered under
public record ExporterHandle(ComponentId id) implements PipelineTarget {

    /// Validates that the id is present.
    public ExporterHandle {
        if (id == null) {
            throw new IllegalArgumentException("exporter handle id must be non-null");
        }
    }
}
