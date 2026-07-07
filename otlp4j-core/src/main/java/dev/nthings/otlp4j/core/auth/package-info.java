/// The authentication SPI, dependency-free over the JDK so no transport or wire library appears on
/// this surface. `ServerAuthenticator` is the server-side authenticator extension that decides a
/// request from its `AuthSources` and returns an `AuthResult`, authenticated with a principal or
/// denied with a reason. `ClientCredentials` is the client-side extension that supplies the per-export
/// credential headers an exporter injects. Both occupy the extension component kind, so the transport
/// that invokes them, the components that implement concrete authenticators, and a declarative loader
/// that references them all name these types through core without a new dependency edge.
@NullMarked
package dev.nthings.otlp4j.core.auth;

import org.jspecify.annotations.NullMarked;
