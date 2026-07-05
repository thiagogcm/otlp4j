package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.pipeline.LogsSink;
import dev.nthings.otlp4j.pipeline.MetricsSink;
import dev.nthings.otlp4j.pipeline.ProfilesSink;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.SharedLifecycle;
import dev.nthings.otlp4j.pipeline.OtlpExporter;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.spi.OtlpClient;
import java.lang.ref.Cleaner;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Shared [OtlpExporter] implementation that fans all four signal facets to a single [OtlpClient]
/// and manages shutdown lifecycle. Used by gRPC and HTTP entry points.
///
/// Each facet is a lifecycle-bearing view: draining one facet drains the whole shared channel, so a
/// pipeline auto-collects and drains the exporter through any attached facet with no extra ceremony.
///
/// The channel is reference-counted: when several subscriptions each attach a facet of the same
/// exporter, every subscription retains it once ([SharedLifecycle#retain]) and the channel closes
/// only on the last release, so one subscription's shutdown cannot close it out from under another.
///
/// An exporter garbage-collected without [ClientExporter#shutdown] logs a warning
/// instead of silently leaking its client channel.
public final class ClientExporter implements OtlpExporter, SharedLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ClientExporter.class);

    /// Warns when an exporter is dropped without [ClientExporter#shutdown].
    private static final Cleaner CLEANER = Cleaner.create();

    private static final Consumer<String> DEFAULT_REPORTER = log::warn;

    private final OtlpClient client;
    private final TracesFacet traces;
    private final MetricsFacet metrics;
    private final LogsFacet logs;
    private final ProfilesFacet profiles;

    /// Signal-keyed view of the four facets, backing the generic [#sink(Signal)] accessor. Populated
    /// once at construction and never mutated, so the lookup in [#sink(Signal)] is provably typed.
    private final Map<Signal<?>, Facet<?>> facets = new HashMap<>();

    /// Number of pipeline subscriptions currently sharing this exporter; the channel closes on the
    /// release that brings it back to zero.
    private final AtomicInteger references = new AtomicInteger();

    /// Latches the one-time channel close, so the last release (or a direct shutdown) closes exactly once.
    private final AtomicBoolean closed = new AtomicBoolean();

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
        facets.put(Signal.TRACES, traces);
        facets.put(Signal.METRICS, metrics);
        facets.put(Signal.LOGS, logs);
        facets.put(Signal.PROFILES, profiles);
        this.leakWatch = new LeakWatch(exporterType, leakReporter);
        this.cleanable = CLEANER.register(this, leakWatch);
    }

    @Override
    public <T> Sink<T> sink(Signal<T> signal) {
        // The map is keyed by Signal identity and each entry's Facet<?> was created with that same
        // signal, so the value's element type is exactly T.
        @SuppressWarnings("unchecked")
        var facet = (Sink<T>) facets.get(signal);
        return facet;
    }

    @Override
    public TracesSink traces() {
        return traces;
    }

    @Override
    public MetricsSink metrics() {
        return metrics;
    }

    @Override
    public LogsSink logs() {
        return logs;
    }

    @Override
    public ProfilesSink profiles() {
        return profiles;
    }

    @Override
    public CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void retain() {
        references.incrementAndGet();
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        // Close the channel only on the last release; a direct, un-retained shutdown (facet used
        // outside a pipeline, or close()) floors the counter at zero and closes.
        if (references.updateAndGet(n -> n > 0 ? n - 1 : 0) > 0) {
            return CompletableFuture.completedFuture(null);
        }
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        // Shutdown was requested; silence the leak watch even if the close below fails or times out.
        leakWatch.closed = true;
        cleanable.clean();
        var closing = new CompletableFuture<Void>();
        var closer = Thread.ofVirtual().name("otlp-exporter-shutdown").start(() -> {
            try {
                client.close();
                closing.complete(null);
            } catch (Throwable t) {
                closing.completeExceptionally(t);
            }
        });
        return closing
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS)
                .whenComplete((v, e) -> {
                    if (e instanceof TimeoutException) {
                        log.warn("exporter close exceeded {}; interrupting transport teardown", timeout);
                        closer.interrupt();
                    }
                });
    }

    /// A lifecycle-bearing view of one signal: `retain`/`shutdown` count against the whole exporter,
    /// since every facet shares its channel. `consume` exports through the shared client via the
    /// carried [Signal]; the four subclasses exist only to carry the marker sink type.
    private abstract class Facet<T> implements SharedLifecycle, Sink<T> {
        final Signal<T> signal;

        Facet(Signal<T> signal) {
            this.signal = signal;
        }

        @Override
        public CompletionStage<ConsumeResult> consume(T batch) {
            return client.export(signal.batch(batch));
        }

        @Override
        public void retain() {
            ClientExporter.this.retain();
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            return ClientExporter.this.shutdown(timeout);
        }
    }

    private final class TracesFacet extends Facet<TracesData> implements TracesSink {
        TracesFacet() {
            super(Signal.TRACES);
        }
    }

    private final class MetricsFacet extends Facet<MetricsData> implements MetricsSink {
        MetricsFacet() {
            super(Signal.METRICS);
        }
    }

    private final class LogsFacet extends Facet<LogsData> implements LogsSink {
        LogsFacet() {
            super(Signal.LOGS);
        }
    }

    private final class ProfilesFacet extends Facet<ProfilesData> implements ProfilesSink {
        ProfilesFacet() {
            super(Signal.PROFILES);
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
