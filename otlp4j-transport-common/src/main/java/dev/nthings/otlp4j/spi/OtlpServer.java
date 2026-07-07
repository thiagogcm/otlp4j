package dev.nthings.otlp4j.spi;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// Transport-side server a receiver drives.
///
/// A receiver constructs one (passing a `ServerConfig` and the [Dispatchers] to invoke) and exposes
/// it through the receiver runtime. Lifecycle methods return [CompletionStage] so the receiver can
/// participate in graph shutdown without spawning threads.
public interface OtlpServer {

    /// Starts listening on the host/port given in the `ServerConfig` the server was built with. Use
    /// port `0` for an ephemeral port; read it back with [#port()].
    ///
    /// @throws IOException if the transport fails to bind
    void start() throws IOException;

    /// The port the server is bound to. Returns 0 before [#start] completes.
    ///
    /// @return the bound port, or 0 before start
    int port();

    /// Initiates a graceful shutdown, draining in-flight requests within `timeout`.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes when teardown finishes
    CompletionStage<Void> shutdown(Duration timeout);

    /// Initiates a forceful, best-effort shutdown and returns promptly. A transport may briefly
    /// await its own teardown before completing: the gRPC server awaits up to 5s for the Netty
    /// event loop to drain; the HTTP server returns at once via `stop(0)`. That bounded gRPC wait
    /// is an internal transport detail, documented here as bounded rather than open-ended.
    ///
    /// @return a stage that completes when the forceful teardown finishes
    CompletionStage<Void> shutdownNow();
}
