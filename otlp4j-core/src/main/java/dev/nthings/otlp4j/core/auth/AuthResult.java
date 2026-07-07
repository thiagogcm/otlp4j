package dev.nthings.otlp4j.core.auth;

import java.security.Principal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// The outcome a [ServerAuthenticator] returns for one request: either the request is authenticated,
/// carrying the resolved principal and any authenticator-supplied attributes, or it is denied with a
/// reason. The transport maps a [Denied] to gRPC `UNAUTHENTICATED` and HTTP `401`, and folds an
/// [Authenticated] result's principal and attributes into the request's client metadata.
public sealed interface AuthResult permits AuthResult.Authenticated, AuthResult.Denied {

    /// A successful authentication.
    ///
    /// @param principal  the resolved principal for the request
    /// @param attributes authenticator-supplied attributes, keyed by name with values in order
    record Authenticated(Principal principal, Map<String, List<String>> attributes)
            implements AuthResult {

        /// Validates and defensively copies the components.
        public Authenticated {
            if (principal == null) {
                throw new IllegalArgumentException("principal must be non-null");
            }
            if (attributes == null) {
                throw new IllegalArgumentException("attributes must be non-null; use Map.of()");
            }
            var copy = new LinkedHashMap<String, List<String>>();
            attributes.forEach((name, values) -> copy.put(name, List.copyOf(values)));
            attributes = Collections.unmodifiableMap(copy);
        }
    }

    /// A denied authentication.
    ///
    /// @param reason the human-readable denial reason, for diagnostics and the wire failure text
    record Denied(String reason) implements AuthResult {

        /// Validates that the reason is present.
        public Denied {
            if (reason == null || reason.isBlank()) {
                throw new IllegalArgumentException("denial reason must be non-blank");
            }
        }
    }

    /// A successful authentication carrying just the principal, with no attributes.
    ///
    /// @param principal the resolved principal
    /// @return the authenticated result
    static AuthResult authenticated(Principal principal) {
        return new Authenticated(principal, Map.of());
    }

    /// A denied authentication with the given reason.
    ///
    /// @param reason the denial reason
    /// @return the denied result
    static AuthResult denied(String reason) {
        return new Denied(reason);
    }
}
