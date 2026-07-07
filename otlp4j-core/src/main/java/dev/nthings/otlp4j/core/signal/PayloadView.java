package dev.nthings.otlp4j.core.signal;

/// A closed, extensible selector for the representation a caller wants to read from a
/// [TelemetryPayload]. It is a final class with a private constructor and public static constants, so
/// a later phase adds a new view as a new constant without breaking any caller or forcing a switch to
/// change, and no proto type ever appears on this surface.
///
/// Phase 2 ships two views. [#PROTO_REQUEST] yields the signal's generated OTLP export-request
/// message as an opaque `Object`, which only codec and transport code casts. [#DOMAIN_IMMUTABLE]
/// yields the signal's immutable domain batch as an opaque `Object`, which a caller narrows through
/// `payload.signal().batchType().cast(...)`. Phase 5 adds [#ROUTING_METADATA], the cheap proto-free
/// routing surface the routing and filtering processors read without materialising the domain. One
/// further view, a raw-bytes view behind benchmark evidence, remains reserved for a later phase and is
/// added as a new constant only.
///
/// The type parameter `V` documents the return type at the call site (`Object` for both shipped
/// views) and leaves room for a strongly typed future view without touching the
/// [TelemetryPayload#view] signature.
///
/// @param <V> the type the view returns
public final class PayloadView<V> {

    /// The generated OTLP export-request message, carried as an opaque `Object`. On a proto-backed
    /// payload this is the message the receiver decoded; on a domain-backed payload it is the message
    /// materialised on first request and cached.
    public static final PayloadView<Object> PROTO_REQUEST = new PayloadView<>("PROTO_REQUEST");

    /// The signal's immutable domain batch, carried as an opaque `Object`. On a domain-backed payload
    /// this is the batch in hand; on a proto-backed payload it is the batch materialised on first
    /// request and cached.
    public static final PayloadView<Object> DOMAIN_IMMUTABLE = new PayloadView<>("DOMAIN_IMMUTABLE");

    /// The cheap, proto-free routing surface, a [RoutingMetadata] over the payload's resource-level
    /// attributes, signal, and item count. A proto-backed payload answers this by reading only the
    /// top-level resource wrappers, never decoding spans, data points, or log records, so a routing
    /// or filtering processor keys on a resource attribute without forcing domain materialisation.
    public static final PayloadView<RoutingMetadata> ROUTING_METADATA =
            new PayloadView<>("ROUTING_METADATA");

    private final String name;

    private PayloadView(String name) {
        this.name = name;
    }

    /// The view name (`PROTO_REQUEST`, `DOMAIN_IMMUTABLE`, or `ROUTING_METADATA`), for diagnostics and
    /// for the unknown-view failure message.
    ///
    /// @return the view name
    @Override
    public String toString() {
        return name;
    }
}
