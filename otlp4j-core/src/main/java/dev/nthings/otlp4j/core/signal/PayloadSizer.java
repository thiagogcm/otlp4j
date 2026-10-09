package dev.nthings.otlp4j.core.signal;

/// Weighs a payload in bytes for a component that bounds or caps work by size, such as a batch
/// processor. A sizer answers for every payload it is given: it returns zero for one it cannot weigh.
@FunctionalInterface
public interface PayloadSizer {

    /// The weight of `payload` in bytes.
    ///
    /// @param payload the payload to weigh
    /// @return the non-negative weight, or zero when this sizer cannot weigh the payload
    long sizeOf(Payload<?> payload);

    /// The sizer that takes the [encoded size][Payload#encodedSize()] a payload reports for itself and
    /// weighs a payload without a wire form as zero. It cannot weigh a batch that only exists in the
    /// domain model, and it costs whatever the payload's own answer costs.
    ///
    /// @return the shared instance
    static PayloadSizer reported() {
        return ReportedSize.INSTANCE;
    }
}
