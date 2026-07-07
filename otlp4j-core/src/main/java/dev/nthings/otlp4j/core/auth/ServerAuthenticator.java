package dev.nthings.otlp4j.core.auth;

import dev.nthings.otlp4j.core.component.Extension;

/// The server-side authenticator SPI: an [Extension] that decides whether a request is authenticated.
/// It occupies the extension component kind, so a concrete authenticator is created through an
/// extension factory, registered like any extension, and started before the data path. A receiver
/// that names an authenticator invokes it per request and maps a denial to the wire failure status.
///
/// This is modelled on the Collector `extensionauth.Server.Authenticate(ctx, sources)` interface,
/// with the request material carried by [AuthSources] and the outcome by [AuthResult].
public interface ServerAuthenticator extends Extension {

    /// Authenticates one request from its authentication material.
    ///
    /// @param sources the request headers, gRPC metadata, and any mutual-TLS peer subject
    /// @return the authentication outcome, authenticated with a principal or denied with a reason
    AuthResult authenticate(AuthSources sources);
}
