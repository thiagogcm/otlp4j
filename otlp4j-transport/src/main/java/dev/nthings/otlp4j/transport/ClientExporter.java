package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.transport.internal.ArmeriaOtlpClient;
import dev.nthings.otlp4j.transport.internal.ExporterMetrics;
import dev.nthings.otlp4j.transport.internal.ReceiverConsumeContext;
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
/// [ArmeriaOtlpClient] and manages shutdown lifecycle. The exporter builder and the graph exporter
/// factory both build one of these.
///
/// Each facet is both a [SignalConsumer] over the shared channel and the graph [Exporter] for its
/// signal: consuming a payload exports its batch through the client and returns a [DeliveryResult]
/// attributed to this exporter's identity. Draining any facet drains the whole shared channel, so one
/// exporter serves every signal that terminates at one endpoint. The factory hands each per-signal
/// facet ([#exporterFor(SignalDescriptor)]) to the graph as an [Exporter]; the facet's lifecycle
/// delegates back to this shared exporter.
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

    private final ArmeriaOtlpClient client;
    private final ExporterMetrics metrics;

    /// Signal-keyed view of the four facets, backing the [#consumer(SignalDescriptor)] and
    /// [#exporterFor(SignalDescriptor)] accessors. Populated once at construction and never mutated, so
    /// each lookup is provably typed.
    private final Map<SignalDescriptor<?>, Facet> facets = new HashMap<>();

    /// Number of owners currently sharing this exporter; the channel closes on the release that
    /// brings it back to zero.
    private final AtomicInteger references = new AtomicInteger();

    /// Latches the one-time channel close, so the last release (or a direct shutdown) closes exactly once.
    private final AtomicBoolean closed = new AtomicBoolean();

    private final LeakWatch leakWatch;
    private final Cleaner.Cleanable cleanable;

    /// Creates an exporter over `client` attributed to `id`, writing every boundary-visible instrument
    /// through `metrics`. This is the live construction path shared by the exporter builder and the
    /// graph exporter factory.
    ///
    /// @param client       the wire client the exporter delegates to
    /// @param id           the exporter component identity attributed to every delivery result
    /// @param exporterType the exporter name used in the leak warning
    /// @param metrics      the single instrument owner the send boundary writes through
    public ClientExporter(
            ArmeriaOtlpClient client, ComponentId id, String exporterType, ExporterMetrics metrics) {
        this(client, id, exporterType, metrics, DEFAULT_REPORTER);
    }

    /// Package-private: an exporter over `client` with no self-observability and the default `otlp` id.
    /// The standalone-use and leak-test construction path.
    ClientExporter(ArmeriaOtlpClient client, String exporterType) {
        this(client, ComponentId.parse("otlp"), exporterType, ExporterMetrics.noop(), DEFAULT_REPORTER);
    }

    /// Package-private: redirects the leak warning so a test can observe it without a logging backend.
    /// Reports no metrics ([ExporterMetrics#noop]) and uses the default `otlp` id.
    ClientExporter(ArmeriaOtlpClient client, String exporterType, Consumer<String> leakReporter) {
        this(client, ComponentId.parse("otlp"), exporterType, ExporterMetrics.noop(), leakReporter);
    }

    /// Canonical constructor: `metrics` is the single instrument owner the send boundary writes
    /// through, `leakReporter` receives the leak warning.
    private ClientExporter(ArmeriaOtlpClient client, ComponentId id, String exporterType,
            ExporterMetrics metrics, Consumer<String> leakReporter) {
        this.client = client;
        this.metrics = metrics;
        for (SignalDescriptor<?> signal : SignalDescriptor.values()) {
            facets.put(signal, new Facet(signal, id));
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

    /// The facet that exports `signal`, typed as the graph [Exporter] for that signal. The graph
    /// exporter factory hands one of these to each pipeline terminating at this exporter; the facet's
    /// lifecycle delegates back to this shared exporter, whose reference count keeps the shared channel
    /// open until the last facet shuts down.
    ///
    /// @param signal the signal to export
    /// @param <T>    the payload type
    /// @return the per-signal graph exporter
    public <T extends TelemetryPayload> Exporter<T> exporterFor(SignalDescriptor<T> signal) {
        // The facets are created per SignalDescriptor and each exports through the one shared client;
        // the cast reconciles the payload-bounded exporter type the graph asks for with the
        // signal-keyed facet, exactly as the receiver side reconciles its per-signal consumers.
        @SuppressWarnings("unchecked")
        Exporter<T> facet = (Exporter<T>) (Exporter<?>) facets.get(signal);
        return facet;
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
                // orTimeout + interrupt below is retained as defense-in-depth.
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

    /// A per-signal facet: both a [SignalConsumer] over the shared client and the graph [Exporter] for
    /// its signal. Consuming a payload exports its batch through the shared client and returns a
    /// [DeliveryResult] attributed to this exporter for that signal. Its start is a no-op (the wrapped
    /// exporter is live once built); flush and shutdown delegate to the shared [ClientExporter], whose
    /// reference count closes the channel only after the last facet shuts down.
    private final class Facet implements Exporter<TelemetryPayload> {

        private final SignalDescriptor<?> signal;
        private final Provenance provenance;

        Facet(SignalDescriptor<?> signal, ComponentId id) {
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
            // pass-through proto-backed payload is never decoded to the domain here. recordSend
            // classifies the outcome: a failed stage or a Rejected result counts as a failure.
            long startNanos = System.nanoTime();
            return client.export(data, provenance).whenComplete((result, failure) ->
                    metrics.recordSend(data, System.nanoTime() - startNanos, result, failure));
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> forceFlush(FlushContext context) {
            return ClientExporter.this.forceFlush(context.deadline());
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return ClientExporter.this.shutdown(context.deadline());
        }

        @Override
        public String toString() {
            return "ClientExporter.Facet(" + provenance.componentId() + ", " + signal + ")";
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
