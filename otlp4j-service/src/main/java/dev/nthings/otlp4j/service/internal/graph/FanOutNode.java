package dev.nthings.otlp4j.service.internal.graph;

import dev.nthings.otlp4j.core.signal.SignalDescriptor;

/// The synthetic graph vertex inserted where one producer feeds more than one consumer of the same
/// signal. It makes fan-out a first-class graph node rather than a value the fluent pipeline
/// discovered: the producer's single outgoing edge lands on the fan-out node, and the fan-out node
/// has one outgoing edge per downstream consumer. At runtime the fan-out node merges the per-consumer
/// results and, for a downstream that declares it mutates the payload, is the place a defensive copy
/// is inserted.
///
/// A fan-out node backs no component and has no lifecycle. It is identified by the signal it fans and
/// a sequence number, so distinct fan-out points on the same signal remain distinct vertices.
///
/// @param signal   the signal this fan-out node fans
/// @param sequence a per-build sequence number giving the node its identity
public record FanOutNode(SignalDescriptor<?> signal, int sequence) implements Node {

    /// Validates that the signal is present.
    public FanOutNode {
        if (signal == null) {
            throw new IllegalArgumentException("fan-out node signal must be non-null");
        }
    }

    @Override
    public String label() {
        return "fanout#" + sequence + " (" + signal + ")";
    }
}
