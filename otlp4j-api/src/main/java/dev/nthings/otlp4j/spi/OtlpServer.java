package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.pipeline.Lifecycle;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// Transport-side server a receiver drives.
///
/// A receiver constructs one (passing a `ServerConfig` and the [Dispatchers] to invoke) and exposes
/// it through `Receiver`/`OtlpGrpcReceiver`. Lifecycle methods return [CompletionStage] so the
/// receiver can participate in pipeline shutdown without spawning threads.
public interface OtlpServer extends Lifecycle {

    /// Starts listening on the host/port given in the `ServerConfig` the server was built with. Use
    /// port `0` for an ephemeral port; read it back with [#port()].
    void start() throws IOException;

    /// The port the server is bound to. Returns 0 before [#start] completes.
    int port();

    /// Initiates a graceful shutdown.
    @Override
    CompletionStage<Void> shutdown(Duration timeout);

    /// Initiates a forceful, best-effort shutdown and returns promptly. A transport may briefly
    /// await its own teardown before completing: the gRPC server awaits up to 5s for the Netty
    /// event loop to drain; the HTTP server returns at once via `stop(0)`. That bounded gRPC wait
    /// is an internal transport detail, documented here as bounded rather than open-ended.
    ///
    /// @return a stage that completes when the forceful teardown finishes
    CompletionStage<Void> shutdownNow();
}
