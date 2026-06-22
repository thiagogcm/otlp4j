package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.pipeline.Drainable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// Transport-side server backing a [dev.nthings.otlp4j.receiver.Receiver].
///
/// Implementations are obtained via [OtlpServerProvider]; application code normally uses
/// `OtlpGrpcReceiver` instead. Lifecycle methods return [CompletionStage] so the receiver can
/// participate in pipeline shutdown without spawning threads.
public interface OtlpServer extends Drainable {

    /// Starts listening on the host/port given in the [ServerTransportConfig] passed to the
    /// provider. Use port `0` for an ephemeral port; read it back with [#port()].
    void start() throws IOException;

    /// The port the server is bound to. Returns 0 before [#start] completes.
    int port();

    /// Initiates a graceful shutdown.
    @Override
    CompletionStage<Void> shutdown(Duration timeout);

    /// Initiates a forceful shutdown.
    CompletionStage<Void> shutdownNow();

    /// The default grace period applied by [#close()] when none is supplied.
    Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30);

    /// Closes the server gracefully, like the rest of the API (`Receiver`/`Exporter`/`Subscription`).
    /// Use [#shutdownNow()] to force-kill in-flight requests instead of draining them.
    @Override
    default void close() {
        shutdown(DEFAULT_CLOSE_TIMEOUT).toCompletableFuture().join();
    }
}
