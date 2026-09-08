package dev.nthings.otlp4j.core.consumer;

import java.security.Principal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// The request-scoped metadata a receiver captures and carries into every consume call: the
/// authenticated principal, if any, and the selected request headers or gRPC metadata. This is what
/// makes metadata-based routing and metadata-keyed batching expressible.
///
/// @param principal the authenticated principal, or empty when unauthenticated
/// @param headers   the selected request headers or gRPC metadata, keyed by name, values in order
public record ClientMetadata(Optional<Principal> principal, Map<String, List<String>> headers) {

    private static final ClientMetadata EMPTY = new ClientMetadata(Optional.empty(), Map.of());

    /// Validates and defensively copies the components.
    public ClientMetadata {
        if (principal == null) {
            throw new IllegalArgumentException("principal must be non-null; use Optional.empty()");
        }
        if (headers == null) {
            throw new IllegalArgumentException("headers must be non-null; use Map.of()");
        }
        var copied = new LinkedHashMap<String, List<String>>(headers.size());
        for (var entry : headers.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("header names must be non-null");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("header values must be non-null");
            }
            copied.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        headers = Map.copyOf(copied);
    }

    /// The empty metadata: no principal, no headers.
    ///
    /// @return the shared empty instance
    public static ClientMetadata empty() {
        return EMPTY;
    }
}
