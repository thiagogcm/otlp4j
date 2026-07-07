package dev.nthings.otlp4j.codec;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.signal.PayloadMemoryMode;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.io.IOException;
import java.io.OutputStream;

/// The pass-through payload: it carries the generated OTLP export request the receiver decoded from
/// the wire, together with its [SignalCodec], and materialises the domain batch only if a consumer
/// asks for it. Its authoritative representation is the proto request, so counting, sizing, and
/// serialisation touch no domain, which is what keeps a same-protocol no-mutation pipeline from ever
/// building a `TracesData` and friends.
///
/// The lazily materialised domain batch is cached under a per-instance guard with a `volatile` field,
/// which gives safe publication to peers on other threads. Both the carried proto message and the
/// cached domain record are immutable, so the payload is share-safe and `copyForMutation` returns
/// `this`.
///
/// @param <T>    the domain batch type
/// @param <REQ>  the signal's proto export-request message
/// @param <RESP> the signal's proto export-response message
public final class ProtoRequestPayload<T, REQ extends MessageLite, RESP extends MessageLite>
        implements TelemetryPayload {

    private final SignalCodec<T, REQ, RESP> codec;
    private final REQ request;

    private final Object domainGuard = new Object();
    private volatile T domain;

    ProtoRequestPayload(SignalCodec<T, REQ, RESP> codec, REQ request) {
        if (codec == null) {
            throw new IllegalArgumentException("codec must be non-null");
        }
        if (request == null) {
            throw new IllegalArgumentException("request must be non-null");
        }
        this.codec = codec;
        this.request = request;
    }

    @Override
    public SignalDescriptor<?> signal() {
        return codec.descriptor();
    }

    @Override
    public long itemCount() {
        return codec.protoItemCount(request);
    }

    /// The exact serialized byte length of the carried OTLP export request, from
    /// `MessageLite.getSerializedSize()`, with no domain materialisation and no `byte[]`.
    ///
    /// @return the serialized size in bytes
    @Override
    public long estimatedSerializedSize() {
        return request.getSerializedSize();
    }

    /// The payload's authoritative cheap representation, always [PayloadMemoryMode#PROTO_BACKED] for
    /// this pass-through payload.
    ///
    /// @return the memory mode
    @Override
    public PayloadMemoryMode memoryMode() {
        return PayloadMemoryMode.PROTO_BACKED;
    }

    /// Returns the requested representation. [PayloadView#PROTO_REQUEST] is the carried message at
    /// zero cost; [PayloadView#DOMAIN_IMMUTABLE] is the domain batch, materialised and cached on first
    /// request; [PayloadView#ROUTING_METADATA] is the cheap routing surface read from only the proto
    /// message's top-level resource wrappers, which never materialises the domain. Any other view
    /// fails closed with an `IllegalArgumentException` naming the view.
    ///
    /// @param view the requested view
    /// @param <V>  the view's return type
    /// @return the requested representation
    @Override
    @SuppressWarnings("unchecked")
    public <V> V view(PayloadView<V> view) {
        if (view == PayloadView.PROTO_REQUEST) {
            return (V) request;
        }
        if (view == PayloadView.DOMAIN_IMMUTABLE) {
            return (V) domain();
        }
        if (view == PayloadView.ROUTING_METADATA) {
            return (V) codec.protoRoutingMetadata(request);
        }
        throw new IllegalArgumentException("unsupported payload view: " + view);
    }

    /// Returns a payload safe to mutate without disturbing this instance. The carried proto message
    /// and the cached domain record are immutable, so sharing is safe and this returns `this`.
    ///
    /// @return this payload
    @Override
    public TelemetryPayload copyForMutation() {
        return this;
    }

    /// Serialises the carried OTLP export request to `out` with `MessageLite.writeTo(OutputStream)`,
    /// with no intermediate `byte[]`.
    ///
    /// @param out the stream to write to
    /// @throws IOException if the stream write fails
    @Override
    public void writeTo(OutputStream out) throws IOException {
        request.writeTo(out);
    }

    /// Test seam: whether the domain batch cache has ever been populated. Used by the
    /// no-materialisation proof to assert a pass-through export never builds the domain.
    ///
    /// @return true once the domain batch has been materialised
    boolean domainMaterialised() {
        return domain != null;
    }

    private T domain() {
        var d = domain;
        if (d == null) {
            synchronized (domainGuard) {
                d = domain;
                if (d == null) {
                    d = codec.toDomain(request);
                    domain = d;
                }
            }
        }
        return d;
    }

    @Override
    public String toString() {
        return "ProtoRequestPayload(" + codec.descriptor().signalName() + ")";
    }
}
