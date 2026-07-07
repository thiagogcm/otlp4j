package dev.nthings.otlp4j.codec;

import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.signal.PayloadMemoryMode;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.io.IOException;
import java.io.OutputStream;

/// The domain-carrying payload: it carries an immutable domain batch (a `TracesData` and friends)
/// together with its [SignalCodec], and materialises the proto export request only if it is exported
/// or sized. Its authoritative representation is the domain batch, so counting reads the batch in
/// hand with no materialisation. This is the payload a domain-view processor emits through
/// [Payloads#domain] and the payload the standalone `ClientExporter.export(signal, batch)` builds.
///
/// The lazily materialised proto request is cached under a per-instance guard with a `volatile`
/// field, which gives safe publication to peers on other threads. Both the carried domain record and
/// the cached proto message are immutable, so the payload is share-safe and `copyForMutation` returns
/// `this`.
///
/// Beyond the [TelemetryPayload] contract this class adds the public `batch()` accessor, the
/// zero-cost narrowing a domain-view reader used before the `view` API landed on the interface.
///
/// @param <T>    the domain batch type
/// @param <REQ>  the signal's proto export-request message
/// @param <RESP> the signal's proto export-response message
public final class DomainRequestPayload<T, REQ extends MessageLite, RESP extends MessageLite>
        implements TelemetryPayload {

    private final SignalCodec<T, REQ, RESP> codec;
    private final T batch;

    private final Object requestGuard = new Object();
    private volatile REQ request;

    DomainRequestPayload(SignalCodec<T, REQ, RESP> codec, T batch) {
        if (codec == null) {
            throw new IllegalArgumentException("codec must be non-null");
        }
        if (batch == null) {
            throw new IllegalArgumentException("batch must be non-null");
        }
        this.codec = codec;
        this.batch = batch;
    }

    /// The carried domain batch. Used transitionally where a reader narrows the payload through a
    /// concrete downcast before the `view` API lands on the interface.
    ///
    /// @return the domain batch
    public T batch() {
        return batch;
    }

    @Override
    public SignalDescriptor<?> signal() {
        return codec.descriptor();
    }

    @Override
    public long itemCount() {
        return codec.descriptor().itemCount(batch);
    }

    /// The exact serialized byte length of the OTLP export request this batch would write, from
    /// `MessageLite.getSerializedSize()`. This forces the one proto materialisation, which is cached.
    ///
    /// @return the serialized size in bytes
    @Override
    public long estimatedSerializedSize() {
        return protoRequest().getSerializedSize();
    }

    /// The payload's authoritative cheap representation, always [PayloadMemoryMode#DOMAIN_BACKED] for
    /// this domain-carrying payload.
    ///
    /// @return the memory mode
    @Override
    public PayloadMemoryMode memoryMode() {
        return PayloadMemoryMode.DOMAIN_BACKED;
    }

    /// Returns the requested representation. [PayloadView#DOMAIN_IMMUTABLE] is the carried batch at
    /// zero cost; [PayloadView#PROTO_REQUEST] is the proto request, materialised and cached on first
    /// request; [PayloadView#ROUTING_METADATA] is the cheap routing surface read from only the domain
    /// batch's resource wrappers. Any other view fails closed with an `IllegalArgumentException`
    /// naming the view.
    ///
    /// @param view the requested view
    /// @param <V>  the view's return type
    /// @return the requested representation
    @Override
    @SuppressWarnings("unchecked")
    public <V> V view(PayloadView<V> view) {
        if (view == PayloadView.DOMAIN_IMMUTABLE) {
            return (V) batch;
        }
        if (view == PayloadView.PROTO_REQUEST) {
            return (V) protoRequest();
        }
        if (view == PayloadView.ROUTING_METADATA) {
            return (V) codec.routingMetadata(batch);
        }
        throw new IllegalArgumentException("unsupported payload view: " + view);
    }

    /// Returns a payload safe to mutate without disturbing this instance. The carried domain record
    /// and the cached proto message are immutable, so sharing is safe and this returns `this`.
    ///
    /// @return this payload
    @Override
    public TelemetryPayload copyForMutation() {
        return this;
    }

    /// Serialises the OTLP export request to `out` with `MessageLite.writeTo(OutputStream)`, writing
    /// the one lazily materialised message.
    ///
    /// @param out the stream to write to
    /// @throws IOException if the stream write fails
    @Override
    public void writeTo(OutputStream out) throws IOException {
        protoRequest().writeTo(out);
    }

    private REQ protoRequest() {
        var r = request;
        if (r == null) {
            synchronized (requestGuard) {
                r = request;
                if (r == null) {
                    r = codec.toProto(batch);
                    request = r;
                }
            }
        }
        return r;
    }

    @Override
    public String toString() {
        return "DomainRequestPayload(" + codec.descriptor().signalName() + ", items=" + itemCount() + ")";
    }
}
