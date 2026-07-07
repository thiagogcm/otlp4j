package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.lang.ref.Cleaner;
import java.time.Clock;
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

/// The shared multi-signal exporter runtime that fans all four signal facets to a single
/// [OtlpClient] and manages shutdown lifecycle. Used by gRPC and HTTP entry points and wrapped for
/// the graph by an `ExporterComponentAdapter`.
///
/// Each facet is a [SignalConsumer] over the shared channel: consuming a payload exports its batch
/// through the client and returns a [DeliveryResult] attributed to this exporter's identity. Draining
/// any facet drains the whole shared channel, so one exporter serves every signal that terminates at
/// one endpoint.
///
/// The channel is reference-counted: when several owners each retain the exporter, the channel closes
/// only on the last release, so one owner's shutdown cannot close it out from under another. An
/// exporter garbage-collected without [#shutdown(Duration)] logs a warning instead of silently
/// leaking its client channel.
public final class ClientExporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClientExporter.class);

    /// The grace applied by [#close()] when an exporter is drained through try-with-resources.
    private static final Duration DEFAULT_GRACE = Duration.ofSeconds(10);

    /// Warns when an exporter is dropped without [#shutdown(Duration)].
    private static final Cleaner CLEANER = Cleaner.create();

    private static final Consumer<String> DEFAULT_REPORTER = log::warn;

    private final OtlpClient client;
    private final ExporterMetrics metrics;

    /// Signal-keyed view of the four facets, backing the [#consumer(SignalDescriptor)] accessor.
    /// Populated once at construction and never mutated, so each lookup is provably typed.
    private final Map<SignalDescriptor<?>, Facet<?>> facets = new HashMap<>();

    /// Number of owners currently sharing this exporter; the channel closes on the release that
    /// brings it back to zero.
    private final AtomicInteger references = new AtomicInteger();

    /// Latches the one-time channel close, so the last release (or a direct shutdown) closes exactly once.
    private final AtomicBoolean closed = new AtomicBoolean();

    private final LeakWatch leakWatch;
    private final Cleaner.Cleanable cleanable;

    /// Creates an exporter over `client`, with no self-observability ([GatewayMetrics#noop]).
    /// `exporterType` labels it in the leak warning.
    ///
    /// @param client       the wire client the exporter delegates to
    /// @param exporterType the exporter name used in the leak warning
    public ClientExporter(OtlpClient client, String exporterType) {
        this(client, ComponentId.parse("otlp"), exporterType, GatewayMetrics.noop(), DEFAULT_REPORTER);
    }

    /// Creates an exporter over `client`, reporting per-signal export success/failure to `telemetry`.
    ///
    /// @param client       the wire client the exporter delegates to
    /// @param exporterType the exporter name used in the leak warning
    /// @param telemetry    the registry the export counters report to
    public ClientExporter(OtlpClient client, String exporterType, GatewayMetrics telemetry) {
        this(client, ComponentId.parse("otlp"), exporterType, telemetry, DEFAULT_REPORTER);
    }

    /// Creates an exporter over `client` attributed to `id`, reporting export counters to `telemetry`.
    ///
    /// @param client       the wire client the exporter delegates to
    /// @param id           the exporter component identity attributed to every delivery result
    /// @param exporterType the exporter name used in the leak warning
    /// @param telemetry    the registry the export counters report to
    public ClientExporter(OtlpClient client, ComponentId id, String exporterType, GatewayMetrics telemetry) {
        this(client, id, exporterType, telemetry, DEFAULT_REPORTER);
    }

    /// Package-private: redirects the leak warning so a test can observe it without a logging backend.
    /// Reports no metrics ([GatewayMetrics#noop]).
    ClientExporter(OtlpClient client, String exporterType, Consumer<String> leakReporter) {
        this(client, ComponentId.parse("otlp"), exporterType, GatewayMetrics.noop(), leakReporter);
    }

    /// Package-private: wraps `telemetry` into a fresh [ExporterMetrics] and delegates to the canonical
    /// constructor. Retained so the direct-construction test paths that hand in a [GatewayMetrics] stay
    /// green; the live builder path uses the [ExporterMetrics]-accepting constructor with its single
    /// shared instance.
    ClientExporter(OtlpClient client, ComponentId id, String exporterType, GatewayMetrics telemetry,
            Consumer<String> leakReporter) {
        this(client, id, exporterType, ExporterMetrics.from(telemetry), leakReporter);
    }

    /// Package-private: builds an exporter over the builder's single shared [ExporterMetrics] with the
    /// default leak reporter.
    ClientExporter(OtlpClient client, ComponentId id, String exporterType, ExporterMetrics metrics) {
        this(client, id, exporterType, metrics, DEFAULT_REPORTER);
    }

    /// Package-private canonical constructor: `metrics` is the single instrument owner the send boundary
    /// writes through, `leakReporter` receives the leak warning.
    ClientExporter(OtlpClient client, ComponentId id, String exporterType, ExporterMetrics metrics,
            Consumer<String> leakReporter) {
        this.client = client;
        this.metrics = metrics;
        for (SignalDescriptor<?> signal : SignalDescriptor.values()) {
            facets.put(signal, new Facet<>(signal, id));
        }
        this.leakWatch = new LeakWatch(exporterType, leakReporter);
        this.cleanable = CLEANER.register(this, leakWatch);
    }

    /// The facet that exports `signal`, typed as a [SignalConsumer] over the payload marker.
    ///
    /// @param signal the signal to export
    /// @return the per-signal facet
    public SignalConsumer<TelemetryPayload> consumer(SignalDescriptor<?> signal) {
        return facets.get(signal);
    }

    /// Exports one `batch` of `signal` through the shared channel under a default delivery context,
    /// the convenience the standalone exporter offers callers that hold a batch rather than a graph
    /// payload.
    ///
    /// @param signal the signal to export
    /// @param batch  the domain batch
    /// @param <T>    the domain batch type
    /// @return a stage completing with the delivery result
    public <T> CompletionStage<DeliveryResult> export(SignalDescriptor<T> signal, T batch) {
        return consumer(signal).consume(
                new ReceiverConsumeContext(Clock.systemUTC()), Payloads.domain(signal, batch));
    }

    /// Flushes any buffered exports; a no-op, since the exporter does not buffer.
    ///
    /// @param timeout the flush deadline
    /// @return a completed stage
    public CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Retains one owner's stake in the shared channel; the channel closes only on the matching
    /// [#release()].
    public void retain() {
        references.incrementAndGet();
    }

    /// Drops one owner's stake without draining; floors at zero. The leak watch remains the safety
    /// net if every owner releases and none shuts the channel down.
    public void release() {
        references.updateAndGet(n -> n > 0 ? n - 1 : 0);
    }

    /// Drains the shared channel within `timeout`, closing it only on the release that brings the
    /// reference count back to zero.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes when teardown finishes
    public CompletionStage<Void> shutdown(Duration timeout) {
        // Close the channel only on the last release; a direct, un-retained shutdown floors the
        // counter at zero and closes.
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
                // Inner bound: the client's own shutdown honours the same deadline. The outer
                // orTimeout + interrupt below is retained as defense-in-depth, so a buggy
                // third-party OtlpClient that ignores its budget is still capped by the caller's.
                client.shutdown(timeout).toCompletableFuture().join();
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

    /// Drains the shared channel with the default grace, swallowing and warning on a teardown failure
    /// so a throw during try-with-resources cannot mask the primary exception.
    @Override
    public void close() {
        try {
            shutdown(DEFAULT_GRACE).toCompletableFuture().join();
        } catch (RuntimeException e) {
            log.warn("exporter close failed", e);
        }
    }

    /// A [SignalConsumer] view of one signal: consuming a payload exports its batch through the shared
    /// client and returns a [DeliveryResult] attributed to this exporter for that signal.
    ///
    /// @param <T> the domain batch type
    private final class Facet<T> implements SignalConsumer<TelemetryPayload> {

        private final SignalDescriptor<T> signal;
        private final Provenance provenance;

        Facet(SignalDescriptor<T> signal, ComponentId id) {
            this.signal = signal;
            this.provenance = new Provenance(id, ComponentKind.EXPORTER, signal);
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
            // Forward the payload as is: the client reads its wire representation directly, so a
            // pass-through proto-backed payload is never decoded to the domain here.
            // Time the send around the client call and feed every boundary-visible instrument through
            // the single owner without altering the returned stage. recordSend classifies the outcome:
            // a failed stage or a Rejected result counts as a failure, anything else as a success.
            long startNanos = System.nanoTime();
            return client.export(data, provenance).whenComplete((result, failure) ->
                    metrics.recordSend(data, System.nanoTime() - startNanos, result, failure));
        }
    }

    /// [Cleaner] action that warns when the exporter is garbage-collected without
    /// [ClientExporter#shutdown]. Holds no reference back to the exporter.
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
                        + "client channel leaked. Drain it through the graph runtime, retain and "
                        + "release it around a shared use, or close it directly when it is used "
                        + "standalone.");
            }
        }
    }
}
