package dev.nthings.otlp4j.codec;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Qualified-internal bridge between typed core payloads and OTLP protobuf requests.
 *
 * <p>This class is exported only to the bundled transport module. It intentionally
 * does not belong to the application API: core payloads never expose wire representations, request
 * serialization, or generated protobuf types.
 */
public final class PayloadWire {

    private PayloadWire() {}

    /** Creates a memoized domain payload for an internal codec path. */
    public static <T, REQ extends MessageLite, RESP extends MessageLite> Payload<T> domain(
            SignalCodec<T, REQ, RESP> codec, T data) {
        return new DomainRequestPayload<>(codec, data);
    }

    /** Wraps a decoded request for lazy domain conversion and wire pass-through. */
    public static <T, REQ extends MessageLite, RESP extends MessageLite> Payload<T> proto(
            SignalCodec<T, REQ, RESP> codec, REQ request) {
        return new ProtoRequestPayload<>(codec, request);
    }

    /**
     * Gives an arbitrary core payload a memoized codec boundary for repeated wire operations.
     * Already codec-backed payloads are returned unchanged: protobuf requests remain exact
     * pass-through objects, including unknown fields, while domain-backed payloads retain their
     * existing request cache.
     *
     * @param payload the payload to memoize
     * @param <T> the domain batch type
     * @return the original codec-backed payload or a memoized codec-backed wrapper
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> Payload<T> memoize(Payload<T> payload) {
        Objects.requireNonNull(payload, "payload");
        if (payload instanceof ProtoRequestPayload<?, ?, ?>
                || payload instanceof DomainRequestPayload<?, ?, ?>) {
            return payload;
        }
        var codec = SignalCodec.forDescriptor(payload.signal());
        return (Payload<T>) new DomainRequestPayload((SignalCodec) codec, payload.data());
    }

    /** Returns the exact encoded request size, without exposing its protobuf representation. */
    public static long size(Payload<?> payload) {
        Objects.requireNonNull(payload, "payload");
        return sizeUnchecked(payload);
    }

    /** Returns the request selected by {@code codec}, preserving a pass-through request when present. */
    public static <T, REQ extends MessageLite, RESP extends MessageLite> REQ request(
            SignalCodec<T, REQ, RESP> codec, Payload<T> payload) {
        Objects.requireNonNull(codec, "codec");
        Objects.requireNonNull(payload, "payload");
        if (!codec.descriptor().equals(payload.signal())) {
            throw new IllegalArgumentException("payload signal does not match codec descriptor");
        }
        return requestUnchecked(codec, payload);
    }

    /** Writes the exact request selected by {@code codec} without an intermediate byte array. */
    public static <T, REQ extends MessageLite, RESP extends MessageLite> void write(
            SignalCodec<T, REQ, RESP> codec, Payload<T> payload, OutputStream output) throws IOException {
        request(codec, payload).writeTo(Objects.requireNonNull(output, "output"));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static long sizeUnchecked(Payload<?> payload) {
        var codec = SignalCodec.forDescriptor((SignalDescriptor) payload.signal());
        return requestUnchecked((SignalCodec) codec, payload).getSerializedSize();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T, REQ extends MessageLite, RESP extends MessageLite> REQ requestUnchecked(
            SignalCodec<T, REQ, RESP> codec, Payload<T> payload) {
        if (payload instanceof ProtoRequestPayload<?, ?, ?> proto) {
            return (REQ) proto.request();
        }
        if (payload instanceof DomainRequestPayload<?, ?, ?> domain) {
            return (REQ) domain.request();
        }
        return codec.toProto(payload.data());
    }
}
