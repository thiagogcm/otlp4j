package dev.nthings.otlp4j.testing.conformance;

/// Which OTLP transport a concrete conformance suite exercises.
///
/// The abstract suite passes the single value it drives to the seam factory so only that
/// transport boots, which halves per-test port churn, TLS material loading, and socket setup
/// versus standing up both servers for every case.
public enum Protocol {

    /// OTLP over HTTP, driven by the JDK HttpClient wire client.
    HTTP,

    /// OTLP over gRPC, driven by the generated blocking stubs.
    GRPC
}
