package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.exporter.OtlpExporter;
import dev.nthings.otlp4j.spi.OtlpClient;
import java.lang.ref.Cleaner;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Shared [OtlpExporter] implementation: fans all four signal facets to a single [OtlpClient] and
/// owns the shutdown lifecycle. The gRPC and HTTP entry points compose one of these.
///
/// As a backstop, an exporter garbage-collected without a `shutdown()`/`close()` logs a warning
/// rather than silently leaking its client channel.
public final class ClientExporter implements OtlpExporter {

    private static final Logger log = LoggerFactory.getLogger(ClientExporter.class);

    /// Warns when an exporter is dropped without `shutdown()`; see `LeakWatch`.
    private static final Cleaner CLEANER = Cleaner.create();

    private static final Consumer<String> DEFAULT_REPORTER = log::warn;

    private final OtlpClient client;
    private final TraceSink traces;
    private final MetricSink metrics;
    private final LogSink logs;
    private final ProfileSink profiles;

    private final ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("otlp-exporter-shutdown").factory());

    private final LeakWatch leakWatch;
    private final Cleaner.Cleanable cleanable;

    /// Composes an exporter over `client`; `exporterType` labels it in the leak warning.
    public ClientExporter(OtlpClient client, String exporterType) {
        this(client, exporterType, DEFAULT_REPORTER);
    }

    /// Package-private: redirects the leak warning so a test can observe it without a logging backend.
    ClientExporter(OtlpClient client, String exporterType, Consumer<String> leakReporter) {
        this.client = client;
        this.traces = client::exportTraces;
        this.metrics = client::exportMetrics;
        this.logs = client::exportLogs;
        this.profiles = client::exportProfiles;
        this.leakWatch = new LeakWatch(exporterType, leakReporter);
        this.cleanable = CLEANER.register(this, leakWatch);
    }

    @Override
    public TraceSink traces() {
        return traces;
    }

    @Override
    public MetricSink metrics() {
        return metrics;
    }

    @Override
    public LogSink logs() {
        return logs;
    }

    @Override
    public ProfileSink profiles() {
        return profiles;
    }

    @Override
    public CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        // Shutdown was requested; silence the leak watch even if the close below fails or times out.
        leakWatch.closed = true;
        cleanable.clean();
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
                shutdownExecutor.shutdownNow();
            } else {
                shutdownExecutor.shutdown();
            }
        });
    }

    /// Cleaner action that warns when the exporter became unreachable without being shut down. Holds
    /// no reference back to the exporter, which would otherwise keep it reachable forever.
    static final class LeakWatch implements Runnable {

        private final String exporterType;
        private final Consumer<String> reporter;
        volatile boolean closed;

        LeakWatch(String exporterType, Consumer<String> reporter) {
            this.exporterType = exporterType;
            this.reporter = reporter;
        }

        @Override
        public void run() {
            if (!closed) {
                reporter.accept(exporterType + " was garbage-collected before shutdown(); its OTLP "
                        + "client channel leaked. Register it for teardown with Stage.owns(exporter) "
                        + "or Stage.to(facet, exporter), or close it directly when a facet is used "
                        + "outside a pipeline.");
            }
        }
    }
}
