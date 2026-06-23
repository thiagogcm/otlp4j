package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.api.internal.SpiSupport;
import dev.nthings.otlp4j.pipeline.Drainable;
import dev.nthings.otlp4j.pipeline.Flushable;
import dev.nthings.otlp4j.pipeline.LogConsumer;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import dev.nthings.otlp4j.pipeline.ProfileConsumer;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.spi.OtlpClientProvider;
import dev.nthings.otlp4j.spi.Protocol;
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

/// Shared lifecycle for the OTLP exporters. One instance handles all four signals: the per-signal
/// facets ([#traces()], [#metrics()], [#logs()], [#profiles()]) are typed `Consumer`s the pipeline
/// attaches to, while lifecycle lives on the exporter itself.
///
/// The concrete [OtlpGrpcExporter] / [OtlpHttpExporter] differ only in the [Protocol] they resolve a
/// transport [OtlpClient] for; everything below — facets, flush, and the cancellation-aware
/// shutdown — is identical and lives here.
///
/// Because the facets are method references, the pipeline cannot auto-discover the exporter behind
/// them — register it explicitly with `Stage.owns(exporter)` so the pipeline drains it on shutdown
/// and flushes it on forceFlush. As a [Drainable] it receives the pipeline's *remaining* shared
/// deadline on shutdown. Shutdown is cancellation-aware: a timeout interrupts the transport teardown
/// rather than leaving it blocking on a background thread past the caller's deadline.
sealed abstract class AbstractOtlpExporter implements Drainable, Flushable
        permits OtlpGrpcExporter, OtlpHttpExporter {

    private static final Logger log = LoggerFactory.getLogger(AbstractOtlpExporter.class);

    private final OtlpClient client;

    /// A single-thread executor this exporter owns, so a shutdown timeout can `shutdownNow()` it to
    /// interrupt the blocking transport close (whose channel/executor awaits are interrupt-aware).
    private final ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor(r -> {
        var t = new Thread(r, "otlp-exporter-shutdown");
        t.setDaemon(true);
        return t;
    });

    AbstractOtlpExporter(ClientTransportConfig config, Protocol protocol) {
        this.client = SpiSupport.provider(OtlpClientProvider.class, protocol).create(config);
        log.debug("created {} exporter for endpoint {}:{}", protocol, config.host(), config.port());
    }

    public final TraceConsumer    traces()   { return client::exportTraces; }
    public final MetricConsumer   metrics()  { return client::exportMetrics; }
    public final LogConsumer      logs()     { return client::exportLogs; }
    public final ProfileConsumer  profiles() { return client::exportProfiles; }

    /// No-op today (the exporter holds no buffer), but reachable: the exporter is [Flushable], so a
    /// pipeline `forceFlush` reaches it once it is registered via `Stage.owns(exporter)`.
    @Override
    public final CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Closes the underlying transport on this exporter's own shutdown thread, completing on a clean
    /// close or `timeout`. On timeout we `shutdownNow()` the executor so the interrupt unblocks the
    /// transport's awaits, honouring the caller's deadline instead of blocking in the background.
    ///
    /// Idempotent: once the close has run the executor is shut down, so a repeat call sees a rejected
    /// submission and returns a completed stage rather than re-closing. This matters because an owned
    /// exporter can be drained by both `Subscription.shutdown` (via [Drainable]) and a later
    /// explicit `close()`.
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
