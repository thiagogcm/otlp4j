package dev.nthings.otlp4j.testing.conformance;

/// The TLS posture a scenario asks the gateway and wire client to take.
///
/// [#MUTUAL] describes a client that presents a certificate. Whether the server requires that
/// certificate is configured by the transport-specific receiver settings.
public enum TlsMode {

    /// Plaintext. No certificate material is loaded and the wire client connects insecurely.
    NONE,

    /// TLS with server authentication only. The client trusts the test CA and verifies the
    /// server certificate; it presents no client certificate.
    SERVER_AUTH,

    /// TLS where the client additionally presents a client certificate. The server ignores it
    /// today, so the request still succeeds; the suite documents that gap as an executable fact.
    MUTUAL
}
