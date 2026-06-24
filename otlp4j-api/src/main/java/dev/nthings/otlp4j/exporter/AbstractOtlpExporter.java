package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.Flushable;
import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.spi.OtlpClient;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Shared lifecycle for the OTLP exporters: one instance handles all four signals through the typed
/// facets ([#traces()], [#metrics()], [#logs()], [#profiles()]). A concrete subclass (in a transport
/// module) supplies the [OtlpClient] it speaks through; the facets, flush and shutdown live here.
///
/// The facets are method references, so a pipeline can't auto-discover the exporter behind them —
/// register it with `Stage.owns(exporter)` to drain/flush it. Shutdown is cancellation-aware: the
/// caller's deadline interrupts the transport teardown rather than leaving it blocking in the background.
public abstract class AbstractOtlpExporter implements Drainable, Flushable {

    private static final Logger log = LoggerFactory.getLogger(AbstractOtlpExporter.class);

    private final OtlpClient client;

    /// A single-thread platform executor this exporter owns, so a shutdown timeout can `shutdownNow()`
    /// it to interrupt the blocking transport close (whose channel/executor awaits are interrupt-aware).
    private final ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("otlp-exporter-shutdown").factory());

    /// Wraps the transport `client` a concrete exporter has built for its protocol.
    protected AbstractOtlpExporter(OtlpClient client) {
        this.client = client;
    }

    public final TraceSink    traces()   { return client::exportTraces; }
    public final MetricSink   metrics()  { return client::exportMetrics; }
    public final LogSink      logs()     { return client::exportLogs; }
    public final ProfileSink  profiles() { return client::exportProfiles; }

    /// No-op: the exporter holds no buffer. Still reachable via a pipeline `forceFlush` once owned.
    @Override
    public final CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Closes the transport on this exporter's own shutdown thread, completing on a clean close or
    /// `timeout`; on timeout we `shutdownNow()` the executor to interrupt the transport's awaits.
    ///
    /// Idempotent: once closed the executor is shut down, so a repeat call sees a rejected submission
    /// and returns completed — an owned exporter can be drained by both the subscription and `close()`.
    @Override
    public final CompletionStage<Void> shutdown(Duration timeout) {
        CompletableFuture<Void> closing;
        try {
            closing = CompletableFuture.runAsync(client::close, shutdownExecutor);
        } catch (RejectedExecutionException alreadyClosed) {
            return CompletableFuture.completedFuture(null);
        }
        closing = closing.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
        return closing.whenComplete((v, e) -> {
            if (e instanceof TimeoutException) {
                log.warn("exporter close exceeded {}; interrupting transport teardown", timeout);
                shutdownExecutor.shutdownNow(); // interrupts client.close() (channel/executor awaits respond to interrupt)
            } else {
                shutdownExecutor.shutdown();
            }
        });
    }

    // close() is inherited from Drainable: shutdown(10s) on this exporter's own shutdown thread.
}
