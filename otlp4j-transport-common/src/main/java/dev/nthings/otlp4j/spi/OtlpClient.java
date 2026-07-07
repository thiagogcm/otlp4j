package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// Transport-side client used by exporters.
///
/// Asynchronous so callers can fan out without spawning threads themselves. Application code
/// normally uses `OtlpGrpcExporter` rather than this SPI directly.
///
/// The client speaks the graph delivery vocabulary directly: an export completes with a
/// [DeliveryResult] carrying the given [Provenance], or fails its stage exceptionally on a transport
/// error (a gRPC status or an HTTP 5xx), which the graph boundary normalizes. [#shutdown(Duration)]
/// drains in-flight exports within a deadline; [#forceFlush(Duration)] is the no-op default, since a
/// client does not buffer.
public interface OtlpClient extends AutoCloseable {

    /// Exports one `payload` and completes when the endpoint acknowledges, mapping the endpoint's
    /// response to a [DeliveryResult] attributed to `provenance`. The client obtains the wire
    /// representation from the payload, so a proto-backed pass-through payload writes with no
    /// re-encode and a domain-backed payload materialises its proto once.
    ///
    /// @param payload    the payload to export
    /// @param provenance the provenance to attribute the result to
    /// @return a stage completing with the export outcome
    CompletionStage<DeliveryResult> export(TelemetryPayload payload, Provenance provenance);

    /// Drains in-flight exports and releases the client's transport resources within `timeout`.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes when teardown finishes
    CompletionStage<Void> shutdown(Duration timeout);

    /// Flushes any buffered exports within `timeout`. The default is a no-op, since a client does not
    /// buffer.
    ///
    /// @param timeout the flush deadline
    /// @return a stage that completes when the flush finishes
    default CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Drains the client with a default ten-second grace, routing through [#shutdown(Duration)] so a
    /// client is usable in a try-with-resources block. A teardown failure is swallowed so a throw
    /// during close cannot mask a primary exception.
    @Override
    default void close() {
        try {
            shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
        } catch (RuntimeException ignored) {
            // best-effort drain
        }
    }
}
