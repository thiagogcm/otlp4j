package dev.nthings.otlp4j.core.signal;

/**
 * One immutable, typed unit of telemetry flowing through a pipeline.
 *
 * <p>The domain batch is the only representation exposed by the core API. Transport and codec
 * implementations may provide a payload whose {@link #data()} is decoded lazily, while cheap
 * accounting and routing metadata remain available without forcing that decode. Replacing the
 * batch always produces a payload for the same signal and recomputes its derived metadata.
 *
 * @param <T> the signal's domain batch type
 */
public interface Payload<T> {

    /**
     * Creates an immutable domain-backed payload.
     *
     * @param signal the signal descriptor
     * @param data the domain batch
     * @param <T> the domain batch type
     * @return a payload carrying {@code data}
     */
    static <T> Payload<T> of(SignalDescriptor<T> signal, T data) {
        return new DomainPayload<>(signal, data);
    }

    /**
     * The signal carried by this payload.
     *
     * @return the signal descriptor
     */
    SignalDescriptor<T> signal();

    /**
     * Returns the domain batch, decoding it lazily when the payload is backed by a wire request.
     *
     * @return the domain batch
     */
    T data();

    /**
     * Returns the number of signal items in this payload. Implementations backed by a wire request
     * should answer this without materialising {@link #data()}.
     *
     * @return the item count
     */
    long itemCount();

    /**
     * Returns resource-level metadata used by routing and filtering. Implementations backed by a
     * wire request should answer this without materialising {@link #data()}.
     *
     * @return immutable routing metadata
     */
    RoutingMetadata routingMetadata();

    /**
     * Returns an immutable payload carrying {@code replacement} for this payload's signal.
     *
     * <p>The replacement's item count and routing metadata are derived afresh. Implementations may
     * return themselves when the replacement is the same object.
     *
     * @param replacement the replacement domain batch
     * @return a payload carrying the replacement
     */
    Payload<T> withData(T replacement);

}
