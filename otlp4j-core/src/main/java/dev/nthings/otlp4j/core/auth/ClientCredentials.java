package dev.nthings.otlp4j.core.auth;

import dev.nthings.otlp4j.core.component.Extension;
import java.util.Map;

/// The client-side credential SPI: an [Extension] that supplies the per-export credential headers an
/// exporter injects, such as a bearer token or a basic-auth header. It occupies the extension
/// component kind, so a concrete credential is created through an extension factory and started before
/// the data path. An exporter that names a credential overlays its headers onto the static request
/// headers per export.
///
/// This collapses the Collector client-auth interfaces to a single header supplier, because both otlp4j
/// transports inject credentials the same way, by overlaying headers on the outgoing request. A
/// refreshing credential returns a fresh header set each call.
public interface ClientCredentials extends Extension {

    /// The credential headers to overlay on an outgoing export, keyed by name. Called once per export,
    /// so a refreshing credential can return a freshly minted token each time.
    ///
    /// @return the credential headers
    Map<String, String> headers();
}
