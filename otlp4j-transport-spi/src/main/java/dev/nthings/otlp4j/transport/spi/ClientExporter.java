package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.exporter.OtlpExporter;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
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

/// Shared [OtlpExporter] implementation that fans all four signal facets to a single [OtlpClient]
/// and manages shutdown lifecycle. Used by gRPC and HTTP entry points.
///
/// Each facet is a lifecycle-bearing view: draining one facet drains the whole shared channel, so a
/// pipeline auto-collects and drains the exporter through any attached facet with no extra ceremony.
///
/// An exporter garbage-collected without [ClientExporter#shutdown] logs a warning
/// instead of silently leaking its client channel.
public final class ClientExporter implements OtlpExporter {

    private static final Logger log = LoggerFactory.getLogger(ClientExporter.class);

    /// Warns when an exporter is dropped without [ClientExporter#shutdown].
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

    /// Creates an exporter over `client`. `exporterType` labels it in the leak warning.
    public ClientExporter(OtlpClient client, String exporterType) {
        this(client, exporterType, DEFAULT_REPORTER);
    }

    /// Package-private: redirects the leak warning so a test can observe it without a logging backend.
    ClientExporter(OtlpClient client, String exporterType, Consumer<String> leakReporter) {
        this.client = client;
        this.traces = new TracesFacet();
        this.metrics = new MetricsFacet();
        this.logs = new LogsFacet();
        this.profiles = new ProfilesFacet();
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

    /// A lifecycle-bearing view of one signal: `shutdown` drains the whole exporter, since every
    /// facet shares its channel. Each subclass adds a `consume` that hits the shared client.
    private abstract class Facet implements Drainable {
        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            return ClientExporter.this.shutdown(timeout);
        }
    }

    private final class TracesFacet extends Facet implements TraceSink {
        @Override
        public CompletionStage<ConsumeResult<TracesData>> consume(TracesData batch) {
            return client.exportTraces(batch);
        }
    }

    private final class MetricsFacet extends Facet implements MetricSink {
        @Override
        public CompletionStage<ConsumeResult<MetricsData>> consume(MetricsData batch) {
            return client.exportMetrics(batch);
        }
    }

    private final class LogsFacet extends Facet implements LogSink {
        @Override
        public CompletionStage<ConsumeResult<LogsData>> consume(LogsData batch) {
            return client.exportLogs(batch);
        }
    }

    private final class ProfilesFacet extends Facet implements ProfileSink {
        @Override
        public CompletionStage<ConsumeResult<ProfilesData>> consume(ProfilesData batch) {
            return client.exportProfiles(batch);
        }
    }

    /// [Cleaner] action that warns when the exporter is garbage-collected without [ClientExporter#shutdown].
    /// Holds no reference back to the exporter.
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
                        + "client channel leaked. Attach a facet to a pipeline, which drains it on "
                        + "shutdown; register it with Stage.owns(exporter) when it is hidden behind a "
                        + "connector or lambda; or close it directly when a facet is used outside a "
                        + "pipeline.");
            }
        }
    }
}
