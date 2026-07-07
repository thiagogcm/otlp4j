package dev.nthings.otlp4j.core.auth;

import java.security.Principal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// An immutable view of a request's authentication material, handed to a [ServerAuthenticator]. It
/// carries the request headers or gRPC metadata, keyed by name with values in wire order, and the
/// mutual-TLS peer-certificate subject when the connection presented a client certificate. This
/// mirrors the Collector server authenticator's `sources` input, generalised to also carry the TLS
/// subject.
///
/// @param headers                 the request headers or gRPC metadata, keyed by name, values in order
/// @param peerCertificatePrincipal the mutual-TLS peer subject, or empty when none was presented
public record AuthSources(Map<String, List<String>> headers,
                          Optional<Principal> peerCertificatePrincipal) {

    /// Validates and defensively copies the components. The header map and every value list are copied,
    /// so the sources are immutable.
    public AuthSources {
        if (headers == null) {
            throw new IllegalArgumentException("headers must be non-null; use Map.of()");
        }
        if (peerCertificatePrincipal == null) {
            throw new IllegalArgumentException(
                    "peerCertificatePrincipal must be non-null; use Optional.empty()");
        }
        var copy = new LinkedHashMap<String, List<String>>();
        headers.forEach((name, values) -> copy.put(name, List.copyOf(values)));
        headers = Collections.unmodifiableMap(copy);
    }

    /// Sources carrying only headers, with no peer certificate.
    ///
    /// @param headers the request headers or gRPC metadata
    /// @return the sources
    public static AuthSources ofHeaders(Map<String, List<String>> headers) {
        return new AuthSources(headers, Optional.empty());
    }
}
