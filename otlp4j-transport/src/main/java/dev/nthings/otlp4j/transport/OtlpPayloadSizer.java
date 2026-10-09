package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.codec.PayloadWire;
import dev.nthings.otlp4j.core.signal.PayloadSizer;

/// Exact OTLP sizing for components that bound or cap work in bytes, such as the batch processor.
public final class OtlpPayloadSizer {

    private static final PayloadSizer EXACT = PayloadWire::size;

    private OtlpPayloadSizer() {}

    /// A sizer that returns the exact serialized size of each payload's OTLP export request, including
    /// its resource and scope grouping. A payload that carries its request answers from it. Any other
    /// payload is encoded to be measured and the encoding is discarded, so weighing a batch built in
    /// process costs one encode.
    ///
    /// @return the shared exact OTLP sizer
    public static PayloadSizer exact() {
        return EXACT;
    }
}
