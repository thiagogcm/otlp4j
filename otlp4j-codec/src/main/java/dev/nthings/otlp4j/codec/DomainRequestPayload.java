package dev.nthings.otlp4j.codec;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.Objects;

/// A codec-backed domain payload. The generated request is encoded lazily and memoized so repeated
/// exporters or retries share one immutable wire message.
final class DomainRequestPayload<T, REQ extends MessageLite, RESP extends MessageLite> implements Payload<T> {

    private final SignalCodec<T, REQ, RESP> codec;
    private final T data;
    private final RoutingMetadata routingMetadata;
    private volatile REQ request;

    DomainRequestPayload(SignalCodec<T, REQ, RESP> codec, T data) {
        this.codec = Objects.requireNonNull(codec, "codec");
        this.data = Objects.requireNonNull(data, "data");
        this.routingMetadata = codec.routingMetadata(data);
    }

    @Override
    public SignalDescriptor<T> signal() {
        return codec.descriptor();
    }

    @Override
    public T data() {
        return data;
    }

    @Override
    public long itemCount() {
        return codec.descriptor().itemCount(data);
    }

    @Override
    public RoutingMetadata routingMetadata() {
        return routingMetadata;
    }

    @Override
    public Payload<T> withData(T replacement) {
        Objects.requireNonNull(replacement, "replacement");
        return replacement == data ? this : new DomainRequestPayload<>(codec, replacement);
    }

    REQ request() {
        var value = request;
        if (value == null) {
            synchronized (this) {
                value = request;
                if (value == null) {
                    value = codec.toProto(data);
                    request = value;
                }
            }
        }
        return value;
    }
}
