package dev.nthings.otlp4j.service.internal.graph;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.signal.SignalPair;

/// The graph vertex for a connector, identified by its [ComponentId] together with the signal pair it
/// bridges. A connector is a shared component that spans two pipelines: it is the exporter of its
/// exporting pipeline (consuming the pair's input signal) and the receiver of its receiving pipeline
/// (emitting the pair's output signal). One connector node bridges the pipelines it joins, which is
/// where a connector cycle becomes representable and is rejected at edge insertion. A node carries
/// exactly one signal pair, so a connector whose exporting pipelines span more than one distinct input
/// signal, or whose receiving pipelines span more than one distinct output signal, is rejected during
/// validation rather than collapsed onto a single mislabeled pair.
///
/// @param id   the connector's identity
/// @param pair the signal pair this connector bridges (input to output)
public record ConnectorNode(ComponentId id, SignalPair<?, ?> pair) implements Node {

    /// Validates that both components are present.
    public ConnectorNode {
        if (id == null || pair == null) {
            throw new IllegalArgumentException("connector node components must be non-null");
        }
    }

    @Override
    public String label() {
        return "connector " + id + " (" + pair + ")";
    }
}
