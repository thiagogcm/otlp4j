package dev.nthings.otlp4j.codec;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.Objects;
import java.util.OptionalLong;

/// A pass-through payload carrying a decoded OTLP request. Its domain representation is decoded lazily
/// only when a component explicitly calls [#data()], preserving receive-to-export forwarding without a
/// domain allocation.
final class ProtoRequestPayload<T, REQ extends MessageLite, RESP extends MessageLite> implements Payload<T> {

    private final SignalCodec<T, REQ, RESP> codec;
    private final REQ request;
    private final RoutingMetadata routingMetadata;
    private volatile T data;

    ProtoRequestPayload(SignalCodec<T, REQ, RESP> codec, REQ request) {
        this.codec = Objects.requireNonNull(codec, "codec");
        this.request = Objects.requireNonNull(request, "request");
        this.routingMetadata = codec.protoRoutingMetadata(request);
    }

    @Override
    public SignalDescriptor<T> signal() {
        return codec.descriptor();
    }

    @Override
    public T data() {
        var value = data;
        if (value == null) {
            synchronized (this) {
                value = data;
                if (value == null) {
                    value = codec.toDomain(request);
                    data = value;
                }
            }
        }
        return value;
    }

    @Override
    public long itemCount() {
        return codec.protoItemCount(request);
    }

    @Override
    public RoutingMetadata routingMetadata() {
        return routingMetadata;
    }

    @Override
    public OptionalLong encodedSize() {
        return OptionalLong.of(request.getSerializedSize());
    }

    @Override
    public Payload<T> withData(T replacement) {
        Objects.requireNonNull(replacement, "replacement");
        return replacement == data ? this : new DomainRequestPayload<>(codec, replacement);
    }

    REQ request() {
        return request;
    }

    boolean dataMaterialized() {
        return data != null;
    }
}
