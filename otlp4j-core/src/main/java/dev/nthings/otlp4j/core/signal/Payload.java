package dev.nthings.otlp4j.core.signal;

import java.util.OptionalLong;

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
     * Returns the exact size in bytes of this payload's OTLP encoding, when the payload has a wire
     * form. A payload received over OTLP answers from its request at no cost. A payload that replaced
     * one answers by encoding its batch once and keeping the result, so the call can be expensive. A
     * payload built from a domain batch alone has no wire form and returns empty.
     *
     * @return the encoded size, or empty when this payload has no wire form
     */
    default OptionalLong encodedSize() {
        return OptionalLong.empty();
    }

    /**
     * Returns an immutable payload carrying {@code replacement} for this payload's signal.
     *
     * <p>The replacement's item count and routing metadata are derived afresh. Implementations may
     * return themselves when the replacement is the same object. A payload that reports an
     * {@linkplain #encodedSize() encoded size} returns a replacement that reports one too.
     *
     * @param replacement the replacement domain batch
     * @return a payload carrying the replacement
     */
    Payload<T> withData(T replacement);

}
