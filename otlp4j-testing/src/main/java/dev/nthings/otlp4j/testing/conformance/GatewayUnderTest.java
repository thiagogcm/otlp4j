package dev.nthings.otlp4j.testing.conformance;

import java.net.URI;
import java.util.Optional;

/// The single seam between the wire conformance suite and a concrete otlp4j build.
///
/// The suite drives whatever this returns purely over HTTP and gRPC; it never names a
/// receiver, exporter, pipeline, or result type. A concrete implementation lives in
/// otlp4j-conformance and is the only place transport and model types appear. When Phase 1
/// replaces the fluent runtime with the service graph, only that implementation changes; every
/// test in this package compiles and passes unchanged.
public interface GatewayUnderTest extends AutoCloseable {

    /// The base URI of the OTLP/HTTP endpoint, for example `https://127.0.0.1:53412`. The four
    /// signal paths are appended by the HTTP wire client. Present only when the concrete suite
    /// requested the HTTP transport; empty otherwise.
    ///
    /// @return the HTTP base URI, or empty when HTTP was not booted
    Optional<URI> httpBaseUri();

    /// The OTLP/gRPC target, for example `127.0.0.1:53413`, consumed by `Grpc.newChannelBuilder`.
    /// Present only when the concrete suite requested the gRPC transport; empty otherwise.
    ///
    /// @return the gRPC target, or empty when gRPC was not booted
    Optional<String> grpcTarget();

    /// The PEM bytes the wire clients must trust to verify the server certificate, present only
    /// when the scenario enabled TLS. Empty for a plaintext scenario.
    ///
    /// @return the server trust material as PEM bytes, or empty for plaintext
    Optional<byte[]> serverTrustPem();

    /// Blocks until the request the scenario asked the gateway to hold has actually acquired its
    /// admission permit and is being held open. Calling this before sending a probe request makes
    /// "the held request occupies the permit" a happens-before fact rather than a scheduling race,
    /// so a probe cannot win the single permit ahead of the intended held request. Returns
    /// immediately when the scenario did not request a held request.
    void awaitHeldRequest();

    /// Completes any request the scenario asked the gateway to hold (the admission gate). A no-op
    /// when the scenario did not request a held request.
    void releaseHeldRequest();

    @Override
    void close();
}
