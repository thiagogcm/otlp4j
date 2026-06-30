package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.ForceFlushable;
import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.TraceSink;
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

/// Shared lifecycle for the OTLP exporters: one instance handles all four
/// signals through typed facets
/// ([#traces()], [#metrics()], [#logs()], [#profiles()]).
///
/// Signal facets are plain
/// [TraceSink]/[MetricSink]/[LogSink]/[ProfileSink] views that deliver to the
/// transport client. They do not carry lifecycle — register the exporter itself
/// with `Pipeline.Stage.owns(exporter)` or `Stage.to(facet, exporter)` so the
/// subscription drains it, or `close()` it directly when a facet is used
/// outside a pipeline.
///
/// As a backstop, an exporter garbage-collected without a `shutdown()`/`close()` logs a warning
/// rather than silently leaking its client channel.
public abstract class AbstractOtlpExporter implements Drainable, ForceFlushable {

    private static final Logger log = LoggerFactory.getLogger(AbstractOtlpExporter.class);

    /// Warns when an exporter is dropped without `shutdown()`; see `LeakWatch`.
    private static final Cleaner CLEANER = Cleaner.create();

    private static final Consumer<String> DEFAULT_REPORTER = message -> log.warn(message);

    private final OtlpClient client;
    private final TraceSink traces;
    private final MetricSink metrics;
    private final LogSink logs;
    private final ProfileSink profiles;

    private final ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("otlp-exporter-shutdown").factory());

    private final LeakWatch leakWatch;
    private final Cleaner.Cleanable cleanable;

    protected AbstractOtlpExporter(OtlpClient client) {
        this(client, DEFAULT_REPORTER);
    }

    /// Package-private: redirects the leak warning so a test can observe it without a logging backend.
    AbstractOtlpExporter(OtlpClient client, Consumer<String> leakReporter) {
        this.client = client;
        this.traces = client::exportTraces;
        this.metrics = client::exportMetrics;
        this.logs = client::exportLogs;
        this.profiles = client::exportProfiles;
        this.leakWatch = new LeakWatch(getClass().getName(), leakReporter);
        this.cleanable = CLEANER.register(this, leakWatch);
    }

    public final TraceSink traces() {
        return traces;
    }

    public final MetricSink metrics() {
        return metrics;
    }

    public final LogSink logs() {
        return logs;
    }

    public final ProfileSink profiles() {
        return profiles;
    }

    @Override
    public final CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public final CompletionStage<Void> shutdown(Duration timeout) {
        // Shutdown was requested — silence the leak watch even if the close below fails or times out.
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

    /// Cleaner action that warns when the exporter became unreachable without being shut down.
    ///
    /// Holds no reference back to the exporter: capturing it here would keep it reachable forever,
    /// and the warning could never fire.
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
