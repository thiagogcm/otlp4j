package dev.nthings.otlp4j.transport;

/// How the server treats a client certificate during the TLS handshake. The trust anchor that
/// verifies a presented client certificate is the CA supplied through the server's own
/// [Tls.Custom] `trustFile` or [Tls.Inline] `trust`, which the server otherwise ignores.
///
/// It sits on [OtlpReceiverConfig] next to [Tls] and defaults to [#NONE], so a graph-built receiver
/// carries the configured mode just as a fluent-built one does.
public enum ClientAuth {
    /// No client certificate is requested; a client presenting one still succeeds. The default.
    NONE,
    /// A client certificate is requested but not required: a certless client still completes the
    /// handshake, and a presented certificate is verified against the configured trust anchor.
    OPTIONAL,
    /// A client certificate is required: a certless client is rejected at the TLS handshake. This is
    /// the enforced mutual-TLS capability.
    REQUIRE
}
