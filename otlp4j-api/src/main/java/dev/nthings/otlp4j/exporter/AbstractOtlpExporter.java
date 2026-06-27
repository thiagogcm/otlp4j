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

/// Shared lifecycle for the OTLP exporters: one instance handles all four
/// signals through typed facets
/// ([#traces()], [#metrics()], [#logs()], [#profiles()]).
///
/// Signal facets are plain
/// [TraceSink]/[MetricSink]/[LogSink]/[ProfileSink] views that deliver to the
/// transport client. They do not carry lifecycle — register the exporter itself
/// with `Pipeline.Stage.owns(exporter)` or `Stage.to(facet, exporter)` so the
/// subscription drains it.
public abstract class AbstractOtlpExporter implements Drainable, Flushable {

    private static final Logger log = LoggerFactory.getLogger(AbstractOtlpExporter.class);

    private final OtlpClient client;
    private final TraceSink traces;
    private final MetricSink metrics;
    private final LogSink logs;
    private final ProfileSink profiles;

    private final ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("otlp-exporter-shutdown").factory());

    protected AbstractOtlpExporter(OtlpClient client) {
        this.client = client;
        this.traces = client::exportTraces;
        this.metrics = client::exportMetrics;
        this.logs = client::exportLogs;
        this.profiles = client::exportProfiles;
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
}
