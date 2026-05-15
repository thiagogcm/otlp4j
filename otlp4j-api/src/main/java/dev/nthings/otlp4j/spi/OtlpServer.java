package dev.nthings.otlp4j.spi;

import java.io.IOException;
import java.time.Duration;

/// Transport-side server used by `OtlpReceiver`.
///
/// Implementations are obtained through [OtlpServerProvider]; application code normally uses
/// `OtlpReceiver` instead of this SPI directly.
public interface OtlpServer extends AutoCloseable {

    /// Starts listening on the given TCP port; `0` selects an ephemeral port.
    void start(int port) throws IOException;

    /// The port being listened on.
    int port();

    /// Initiates a graceful shutdown; in-flight exports are allowed to complete.
    void shutdown();

    /// Initiates a forceful shutdown; in-flight exports are cancelled.
    void shutdownNow();

    /// Blocks until the server has terminated, or the timeout elapses.
    boolean awaitTermination(Duration timeout) throws InterruptedException;

    /// Blocks until the server has terminated.
    void awaitTermination() throws InterruptedException;

    @Override
    default void close() {
        shutdownNow();
    }
}
