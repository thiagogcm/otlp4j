package dev.nthings.otlp4j.core.consumer;

/// A consumer's declared capabilities. `mutatesData` states whether the consumer mutates the payload
/// it is given. It is declared, not inferred, so the fan-out node can insert a defensive copy before
/// a mutating consumer, following the Collector fan-out rule, rather than rejecting the topology.
///
/// @param mutatesData whether the consumer mutates the payload it receives
public record ConsumerCapabilities(boolean mutatesData) {

    private static final ConsumerCapabilities NON_MUTATING = new ConsumerCapabilities(false);

    private static final ConsumerCapabilities MUTATING = new ConsumerCapabilities(true);

    /// The capabilities of a consumer that does not mutate its input.
    ///
    /// @return non-mutating capabilities
    public static ConsumerCapabilities nonMutating() {
        return NON_MUTATING;
    }

    /// The capabilities of a consumer that mutates its input.
    ///
    /// @return mutating capabilities
    public static ConsumerCapabilities mutating() {
        return MUTATING;
    }
}
