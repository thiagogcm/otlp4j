package dev.nthings.otlp4j.core.signal;

import java.io.IOException;
import java.io.OutputStream;

/// The marker for a unit of telemetry flowing through the graph. Every graph contract is expressed
/// over `SignalConsumer<T extends TelemetryPayload>` and `SignalDescriptor<T>`, so the concrete
/// payload type is never named in a factory, a consumer, the graph, or the runtime.
///
/// Phase 2 widens this from the two-method marker Phase 1 shipped into the full contract: alongside
/// the signal and item count it now exposes the serialized size, a view selector ([PayloadView]) that
/// materialises the requested representation lazily, a mutation-safe copy hook, and a direct
/// serialisation to an [OutputStream]. The interface stays proto-free: the proto request is reached
/// only through the
/// opaque [PayloadView#PROTO_REQUEST] view, whose runtime value the codec and the transports narrow,
/// so no wire type appears on any signature here.
public interface TelemetryPayload {

    /// The signal this payload belongs to.
    ///
    /// @return the signal descriptor
    SignalDescriptor<?> signal();

    /// The number of OTLP items in this payload, for partial-success accounting. A proto-backed
    /// payload answers this by counting the proto message directly, without materialising the domain.
    ///
    /// @return the item count
    long itemCount();

    /// The exact serialized byte length of the OTLP export request this payload would write, from
    /// `getSerializedSize()`. Named "estimated" to match the report, but exact. A proto-backed payload
    /// answers without touching the domain; a domain-backed payload materialises its proto once, which
    /// is cached and reused on export.
    ///
    /// @return the serialized size in bytes
    long estimatedSerializedSize();

    /// Returns the requested representation, materialising and caching it on first request.
    /// [PayloadView#PROTO_REQUEST] yields the OTLP export request; [PayloadView#DOMAIN_IMMUTABLE]
    /// yields the signal's domain batch. An unknown view fails closed with an
    /// `IllegalArgumentException` naming the view.
    ///
    /// @param view the requested view
    /// @param <V>  the view's return type
    /// @return the requested representation
    <V> V view(PayloadView<V> view);

    /// Returns a payload safe to mutate without disturbing this instance. Both Phase 2 backings are
    /// immutable, so this shares the backing and returns a share-safe payload; it is the seam the
    /// fan-out node calls before handing the payload to a mutating peer. A future mutable mode
    /// overrides this to deep copy, leaving the graph call site unchanged.
    ///
    /// @return a payload safe to mutate independently of this one
    TelemetryPayload copyForMutation();

    /// Serialises the OTLP export request to `out` with `writeTo(OutputStream)`, with no intermediate
    /// `byte[]`. A proto-backed payload writes the carried message; a domain-backed payload writes the
    /// one lazily materialised message.
    ///
    /// @param out the stream to write to
    /// @throws IOException if the stream write fails
    void writeTo(OutputStream out) throws IOException;
}
