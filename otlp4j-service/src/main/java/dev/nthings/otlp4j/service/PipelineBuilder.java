package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/// The small typed DSL a [GatewayBuilder#pipeline(dev.nthings.otlp4j.core.pipeline.PipelineId,
/// java.util.function.Consumer)] block receives to wire one pipeline from registration handles rather
/// than raw ids: `p -> p.from(in).via(enrich, redact).to(out, spanCount)`. Every argument is a typed
/// handle, so referencing an unregistered or wrong-kind component is a compile error here, where the
/// raw [PipelineConfig] path leaves it a runtime validation diagnostic.
///
/// The three clauses may each be called at most once and compile straight down to the ordered id lists
/// of a [PipelineConfig].
public final class PipelineBuilder {

    private List<ComponentId> receivers;
    private List<ComponentId> processors;
    private List<ComponentId> exporters;

    private final Object owner;

    PipelineBuilder(Object owner) { this.owner = owner; }

    /// Sets the pipeline's head from receiver and connector-as-receiver handles.
    ///
    /// @param sources the pipeline sources feeding this pipeline
    /// @return this builder
    public PipelineBuilder from(PipelineSource... sources) {
        if (receivers != null) {
            throw new IllegalStateException("from(...) was already set for this pipeline");
        }
        receivers = ids(sources, PipelineSource::id);
        return this;
    }

    /// Sets the pipeline's ordered processor chain.
    ///
    /// @param chain the processor handles, applied in order
    /// @return this builder
    public PipelineBuilder via(ProcessorHandle... chain) {
        if (processors != null) {
            throw new IllegalStateException("via(...) was already set for this pipeline");
        }
        processors = ids(chain, ProcessorHandle::id);
        return this;
    }

    /// Sets the pipeline's terminals from exporter and connector-as-exporter handles.
    ///
    /// @param targets the pipeline targets this pipeline feeds
    /// @return this builder
    public PipelineBuilder to(PipelineTarget... targets) {
        if (exporters != null) {
            throw new IllegalStateException("to(...) was already set for this pipeline");
        }
        exporters = ids(targets, PipelineTarget::id);
        return this;
    }

    PipelineConfig toConfig() {
        return new PipelineConfig(
                receivers == null ? List.of() : receivers,
                processors == null ? List.of() : processors,
                exporters == null ? List.of() : exporters);
    }

    private <H> List<ComponentId> ids(H[] handles, Function<H, ComponentId> id) {
        var result = new ArrayList<ComponentId>(handles.length);
        for (var handle : handles) {
            if (handle == null) {
                throw new IllegalArgumentException("pipeline handle must be non-null");
            }
            Object handleOwner = switch (handle) {
                case ReceiverHandle h -> h.owner();
                case ProcessorHandle h -> h.owner();
                case ExporterHandle h -> h.owner();
                case ConnectorHandle h -> h.owner();
                case InputHandle<?> h -> h.owner();
                default -> throw new IllegalArgumentException("unknown component handle");
            };
            if (handleOwner != owner) {
                throw new IllegalArgumentException("component handle belongs to another builder");
            }
            result.add(id.apply(handle));
        }
        return result;
    }
}
