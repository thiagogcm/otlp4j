package dev.nthings.otlp4j.testing.policy;

import dev.nthings.otlp4j.testing.conformance.Protocol;

/// The single seam between the reusable policy semantics suite and a concrete otlp4j build. A concrete
/// implementation lives in otlp4j-conformance and is the only place a real exporter type appears; it
/// boots a programmed server, drives one export of a neutral [ExportProgram] through a real exporter
/// configured with the program's policies, and returns a neutral [ExportObservation]. The suite drives
/// only this interface and the neutral vocabulary, so the same suite gates the future Armeria transport
/// by supplying a new seam implementation and nothing else.
///
/// The capability methods let the abstract suite switch its assertions without naming a transport. They
/// declare what the transport under test actually does today, so a case that pins a legacy miss now
/// becomes a real assertion on a transport that closes the gap, with no edit to the suite.
public interface PolicyExporterUnderTest extends AutoCloseable {

    /// Boots a programmed server on an ephemeral loopback port, configures a real exporter with the
    /// program's policies pointed at that port, drives exactly one export, waits for the caller stage to
    /// settle, tears the exporter down under a bounded grace, snapshots the server's recorded arrivals,
    /// and returns the observation. A fresh server is booted and closed within this call, so no state
    /// leaks between invocations.
    ///
    /// @param program the neutral program to run
    /// @return the observed behaviour
    ExportObservation runOnce(ExportProgram program);

    /// The single transport this seam drives.
    ///
    /// @return the protocol
    Protocol protocol();

    /// Whether the transport folds a server RetryInfo hint into its backoff. The legacy gRPC transport
    /// does not (it ignores `grpc-status-details-bin` RetryInfo), so it declares false; the future
    /// Armeria transport will declare true. The HTTP transport folds the RetryInfo-equivalent
    /// `Retry-After` and so declares true.
    ///
    /// @return true when the transport folds RetryInfo
    boolean honorsGrpcRetryInfo();

    /// Whether the transport cancels in-flight retry scheduling at the total deadline. The legacy
    /// transports abandon the caller stage at the total budget but do not interrupt an already-scheduled
    /// retry, so they declare false; the future Armeria transport will declare true.
    ///
    /// @return true when the transport cancels retries at the total deadline
    boolean cancelsRetriesAtTotalDeadline();

    @Override
    void close();
}
