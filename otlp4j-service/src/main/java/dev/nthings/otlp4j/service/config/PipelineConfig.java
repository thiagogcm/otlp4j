package dev.nthings.otlp4j.service.config;

import dev.nthings.otlp4j.core.component.ComponentId;
import java.util.List;

/// One pipeline's declarative wiring, the OpenTelemetry Collector pipeline record verbatim: the
/// component ids of its receivers, its ordered processors, and its exporters. The signal a pipeline
/// carries is not held here but by the [dev.nthings.otlp4j.core.pipeline.PipelineId] this config is
/// keyed under in the [GatewayConfig].
///
/// The processor list is ordered, since a processor chain is applied in order. The receiver and
/// exporter lists are unordered sets in spirit but kept as lists to match the Collector record; a
/// receiver id that also names a connector routes that connector as this pipeline's receiver, and an
/// exporter id that names a connector routes that connector as this pipeline's exporter.
///
/// @param receivers  the receiver (or connector-as-receiver) ids feeding this pipeline
/// @param processors the ordered processor ids applied in this pipeline
/// @param exporters  the exporter (or connector-as-exporter) ids this pipeline feeds
public record PipelineConfig(
        List<ComponentId> receivers, List<ComponentId> processors, List<ComponentId> exporters) {

    /// Defensively copies each list and validates that none is null.
    public PipelineConfig {
        if (receivers == null || processors == null || exporters == null) {
            throw new IllegalArgumentException("receivers, processors, and exporters must be non-null");
        }
        receivers = List.copyOf(receivers);
        processors = List.copyOf(processors);
        exporters = List.copyOf(exporters);
    }

    /// A pipeline with the given receivers and exporters and no processors.
    ///
    /// @param receivers the receiver ids
    /// @param exporters the exporter ids
    /// @return the pipeline config
    public static PipelineConfig of(List<ComponentId> receivers, List<ComponentId> exporters) {
        return new PipelineConfig(receivers, List.of(), exporters);
    }
}
