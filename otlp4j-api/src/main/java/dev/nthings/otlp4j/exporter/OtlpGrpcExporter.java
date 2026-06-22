package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.api.internal.SpiSupport;
import dev.nthings.otlp4j.pipeline.Drainable;
import dev.nthings.otlp4j.pipeline.Flushable;
import dev.nthings.otlp4j.pipeline.LogConsumer;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import dev.nthings.otlp4j.pipeline.ProfileConsumer;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.Compression;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.spi.OtlpClientProvider;
import dev.nthings.otlp4j.spi.RetryPolicy;
import dev.nthings.otlp4j.spi.Tls;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Exports typed telemetry to an OTLP/gRPC endpoint.
///
/// One instance handles all four signals. The per-signal facets ([#traces()], [#metrics()],
/// [#logs()], [#profiles()]) are typed `Consumer`s the pipeline attaches to; lifecycle lives on
/// the exporter itself.
///
/// Because the facets are method references, the pipeline cannot auto-discover this exporter behind
/// them — register it explicitly with `Stage.owns(exporter)` so the pipeline drains it on shutdown
/// and flushes it on forceFlush. As a [Drainable] it receives the pipeline's *remaining* shared
/// deadline on shutdown. Shutdown is cancellation-aware: a timeout interrupts the transport teardown
/// rather than leaving it blocking on a background thread past the caller's deadline.
public final class OtlpGrpcExporter implements Drainable, Flushable {

    private static final Logger log = LoggerFactory.getLogger(OtlpGrpcExporter.class);

    private final OtlpClient client;

    /// A single-thread executor this exporter owns, so a shutdown timeout can `shutdownNow()` it to
    /// interrupt the blocking transport close (whose channel/executor awaits are interrupt-aware).
    private final ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor(r -> {
        var t = new Thread(r, "otlp-exporter-shutdown");
        t.setDaemon(true);
        return t;
    });

    private OtlpGrpcExporter(Builder b) {
        var cfg = b.config.build();
        this.client = SpiSupport.firstProvider(OtlpClientProvider.class).create(cfg);
        log.debug("created OTLP/gRPC exporter for endpoint {}:{}", cfg.host(), cfg.port());
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Convenience constructor: build, connect, ready-to-use.
    public static OtlpGrpcExporter to(String host, int port) {
        return builder().endpoint(host, port).build();
    }

    public TraceConsumer    traces()   { return client::exportTraces; }
    public MetricConsumer   metrics()  { return client::exportMetrics; }
    public LogConsumer      logs()     { return client::exportLogs; }
    public ProfileConsumer  profiles() { return client::exportProfiles; }

    /// No-op today (the exporter holds no buffer), but reachable: the exporter is [Flushable], so a
    /// pipeline `forceFlush` reaches it once it is registered via `Stage.owns(exporter)`.
    @Override
    public CompletionStage<Void> forceFlush(Duration timeout) {
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
    public CompletionStage<Void> shutdown(Duration timeout) {
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

    /// Builder for [OtlpGrpcExporter]. Defaults to `localhost:4317` with a 10s deadline.
    public static final class Builder {

        private ClientTransportConfig.Builder config = ClientTransportConfig.builder();

        private Builder() {}

        public Builder transport(ClientTransportConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        public Builder endpoint(String host, int port) {
            config.endpoint(host, port);
            return this;
        }

        public Builder host(String host) {
            config.host(host);
            return this;
        }

        public Builder port(int port) {
            config.port(port);
            return this;
        }

        public Builder timeout(Duration timeout) {
            config.timeout(timeout);
            return this;
        }

        /// Selects the client TLS mode (e.g. [Tls#systemTrust()] or [Tls#custom]). Defaults to
        /// plaintext.
        public Builder tls(Tls tls) {
            config.tls(tls);
            return this;
        }

        /// Adds one request metadata header (e.g. `authorization`) sent on every export.
        public Builder header(String key, String value) {
            config.header(key, value);
            return this;
        }

        /// Adds all of `headers` as request metadata, on top of any already set.
        public Builder headers(Map<String, String> headers) {
            config.headers(headers);
            return this;
        }

        /// Selects request-body compression (e.g. [Compression#GZIP]). Defaults to none.
        public Builder compression(Compression compression) {
            config.compression(compression);
            return this;
        }

        /// Sets the transport retry policy (e.g. [RetryPolicy#exponential]). Defaults to no retries.
        public Builder retry(RetryPolicy retry) {
            config.retry(retry);
            return this;
        }

        public OtlpGrpcExporter build() {
            return new OtlpGrpcExporter(this);
        }
    }
}
