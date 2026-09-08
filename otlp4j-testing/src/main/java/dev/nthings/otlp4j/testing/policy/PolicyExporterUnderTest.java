package dev.nthings.otlp4j.testing.policy;

import dev.nthings.otlp4j.testing.conformance.Protocol;

/// The single seam between the reusable policy semantics suite and a concrete otlp4j build. A concrete
/// implementation lives in otlp4j-conformance and is the only place a real exporter type appears; it
/// boots a programmed server, drives one export of a neutral [ExportProgram] through a real exporter
/// configured with the program's policies, and returns a neutral [ExportObservation]. The suite drives
/// only this interface and the neutral vocabulary, so the same suite can be applied to any transport
/// by supplying a seam implementation and nothing else.
///
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

    @Override
    void close();
}
