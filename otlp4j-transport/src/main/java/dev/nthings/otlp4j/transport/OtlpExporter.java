package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.codec.PayloadWire;
import dev.nthings.otlp4j.transport.internal.ArmeriaOtlpClient;
import dev.nthings.otlp4j.transport.internal.ExporterMetrics;
import dev.nthings.otlp4j.transport.internal.Protocol;
import java.time.Duration;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An owned in-process OTLP exporter.
 *
 * <p>{@code build()} only validates and captures configuration. The wire client is created by
 * {@code start()}, making ownership and startup failures explicit. Exporting a domain batch is
 * type safe through the signal descriptor; graph factories use the signal facet.
 */
public final class OtlpExporter implements AutoCloseable {

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private enum State { NEW, STARTED, STOPPING, STOPPED }

    private final Protocol protocol;
    private final OtlpExporterConfig config;
    private final ComponentId id;
    private final String exporterType;
    private final ExporterMetrics metrics;
    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
    // One wire owner can be represented by several per-signal graph facets. The owner is released
    // only after the last facet has been shut down, so reverse graph traversal cannot stop a shared
    // client while another signal still has buffered data downstream.
    private final AtomicInteger facetReferences = new AtomicInteger();
    private volatile ArmeriaOtlpClient client;
    private volatile CompletionStage<Void> shutdownStage;

    private OtlpExporter(
            Protocol protocol,
            OtlpExporterConfig config,
            ComponentId id,
            String exporterType,
            ExporterMetrics metrics) {
        this.protocol = protocol;
        this.config = config;
        this.id = id;
        this.exporterType = exporterType;
        this.metrics = metrics;
    }

    static OtlpExporter forFactory(
            Protocol protocol,
            OtlpExporterConfig config,
            ComponentId id,
            GatewayMetrics telemetry) {
        return new OtlpExporter(protocol, config, id, protocol == Protocol.GRPC ? "otlp/gRPC" : "otlp/HTTP",
                ExporterMetrics.from(telemetry));
    }

    /** Returns a gRPC exporter builder using port 4317. */
    public static Builder grpcBuilder() {
        return new Builder(Protocol.GRPC, OtlpExporterConfig.builder(4317), "otlp/gRPC");
    }

    /** Returns an OTLP/HTTP exporter builder using port 4318. */
    public static Builder httpBuilder() {
        return new Builder(Protocol.HTTP, OtlpExporterConfig.builder(4318), "otlp/HTTP");
    }

    /** Starts the wire client. Calls after the first successful start are idempotent. */
    public synchronized OtlpExporter start() {
        var current = state.get();
        if (current == State.STARTED) {
            return this;
        }
        if (current != State.NEW) {
            throw new IllegalStateException("exporter is already stopped");
        }
        var created = new ArmeriaOtlpClient(config, metrics, protocol);
        try {
            created.start();
        } catch (RuntimeException | Error failure) {
            created.close();
            throw failure;
        }
        client = created;
        state.set(State.STARTED);
        return this;
    }

    /** Whether the exporter has completed startup and can accept batches. */
    public boolean isStarted() {
        return state.get() == State.STARTED;
    }

    /** Exports one typed domain batch using the default consume context. */
    public <T> CompletionStage<DeliveryResult> export(SignalDescriptor<T> signal, T batch) {
        return export(ConsumeContext.defaults(), Payload.of(signal, batch));
    }

    /** Exports one typed payload. */
    public <T> CompletionStage<DeliveryResult> export(Payload<T> payload) {
        return export(ConsumeContext.defaults(), payload);
    }

    /** Exports one typed domain batch under a caller-supplied delivery context. */
    public <T> CompletionStage<DeliveryResult> export(
            ConsumeContext context, SignalDescriptor<T> signal, T batch) {
        return export(context, Payload.of(signal, batch));
    }

    /** Exports one typed payload under a caller-supplied delivery context. */
    public <T> CompletionStage<DeliveryResult> export(ConsumeContext context, Payload<T> payload) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(payload, "payload");
        if (context.isCancelled()) {
            return CompletableFuture.failedFuture(new CancellationException("export context is cancelled"));
        }
        var remaining = context.deadline().remaining(Clock.systemUTC());
        if (remaining.isNegative() || remaining.isZero()) {
            return CompletableFuture.failedFuture(new CancellationException("export context deadline elapsed"));
        }
        var effectivePayload = PayloadWire.memoize(payload);
        var current = client();
        var provenance = new Provenance(id, ComponentKind.EXPORTER, effectivePayload.signal());
        var startedAt = System.nanoTime();
        var serializedBytes = serializedSize(effectivePayload);
        remaining = context.deadline().remaining(Clock.systemUTC());
        if (context.isCancelled() || remaining.isNegative() || remaining.isZero()) {
            return CompletableFuture.failedFuture(new CancellationException("export context expired during encoding"));
        }
        var total = config.timeout().total();
        var budget = remaining.compareTo(total) < 0 ? remaining : total;
        // The client gates registration against shutdown; encoding need not serialize every caller
        // on the owner's lifecycle monitor.
        var delivery = current.export(effectivePayload, provenance).toCompletableFuture();
        delivery.whenComplete((result, failure) ->
                metrics.recordSend(effectivePayload, serializedBytes, System.nanoTime() - startedAt, result, failure));
        context.onCancel(() -> delivery.cancel(true));
        return delivery.orTimeout(TimeUnit.NANOSECONDS.convert(budget), TimeUnit.NANOSECONDS);
    }

    /** Flushes in-flight exports and returns an empty report because the wire client has no buffer. */
    public CompletionStage<FlushReport> flush(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }
        var current = client;
        return current == null
                ? CompletableFuture.completedFuture(FlushReport.empty())
                : current.flush(timeout);
    }

    /** Drains in-flight exports and releases wire resources within {@code timeout}. */
    public CompletionStage<Void> shutdown(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }
        synchronized (this) {
            if (shutdownStage != null) {
                return shutdownStage;
            }
            if (state.get() == State.STOPPED || state.get() == State.NEW) {
                state.set(State.STOPPED);
                shutdownStage = CompletableFuture.completedFuture(null);
                return shutdownStage;
            }
            state.set(State.STOPPING);
            if (client == null) {
                state.set(State.STOPPED);
                shutdownStage = CompletableFuture.completedFuture(null);
                return shutdownStage;
            }
            var result = client.shutdown(timeout).whenComplete((ignored, failure) -> {
                if (failure == null) {
                    state.set(State.STOPPED);
                }
            });
            shutdownStage = result;
            return result;
        }
    }

    @Override
    public void close() {
        shutdown(DEFAULT_CLOSE_TIMEOUT).toCompletableFuture().join();
    }

    /** Creates a graph exporter facet for one signal. */
    <T> Exporter<T> facet(SignalDescriptor<T> signal) {
        facetReferences.incrementAndGet();
        return new Facet<>(this, signal);
    }

    private CompletionStage<Void> releaseFacet(Duration timeout) {
        var remaining = facetReferences.updateAndGet(count -> count > 0 ? count - 1 : 0);
        return remaining == 0 ? shutdown(timeout) : CompletableFuture.completedFuture(null);
    }

    private ArmeriaOtlpClient client() {
        if (state.get() != State.STARTED || client == null) {
            throw new IllegalStateException("exporter has not been started");
        }
        return client;
    }

    private static long serializedSize(Payload<?> payload) {
        return PayloadWire.size(payload);
    }

    /** Fluent owned exporter builder. */
    public static final class Builder {

        private final Protocol protocol;
        private OtlpExporterConfig.Builder config;
        private final String exporterType;
        private ComponentId id = ComponentId.parse("otlp");
        private GatewayMetrics gatewayMetrics = GatewayMetrics.noop();

        private Builder(Protocol protocol, OtlpExporterConfig.Builder config, String exporterType) {
            this.protocol = protocol;
            this.config = Objects.requireNonNull(config, "config");
            this.exporterType = exporterType;
        }

        public Builder config(OtlpExporterConfig config) {
            this.config = Objects.requireNonNull(config, "config").toBuilder();
            return this;
        }

        public Builder fromEnvironment() {
            config.fromEnvironment();
            return this;
        }

        public Builder endpoint(String url) {
            config.endpoint(url);
            return this;
        }

        public Builder endpoint(String host, int port) {
            config.endpoint(host, port);
            return this;
        }

        public Builder path(String path) {
            config.path(path);
            return this;
        }

        public Builder timeout(Duration timeout) {
            config.timeout(timeout);
            return this;
        }

        public Builder totalTimeout(Duration timeout) {
            config.totalTimeout(timeout);
            return this;
        }

        public Builder tls(Tls tls) {
            config.tls(tls);
            return this;
        }

        public Builder addHeader(String key, String value) {
            config.addHeader(key, value);
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            config.headers(headers);
            return this;
        }

        public Builder headers(Supplier<Map<String, String>> supplier) {
            config.headers(supplier);
            return this;
        }

        public Builder compression(Compression compression) {
            config.compression(compression);
            return this;
        }

        public Builder compression(String compression) {
            config.compression(compression);
            return this;
        }

        public Builder retryPolicy(RetryPolicy retry) {
            config.retryPolicy(retry);
            return this;
        }

        public Builder telemetry(GatewayMetrics telemetry) {
            gatewayMetrics = Objects.requireNonNull(telemetry, "telemetry");
            return this;
        }

        public Builder componentId(ComponentId id) {
            this.id = Objects.requireNonNull(id, "id");
            return this;
        }

        public OtlpExporter build() {
            return new OtlpExporter(
                    protocol, config.build(), id, exporterType, ExporterMetrics.from(gatewayMetrics));
        }
    }

    private static final class Facet<T> implements Exporter<T> {

        private final OtlpExporter owner;
        private final SignalDescriptor<T> signal;
        private final AtomicBoolean released = new AtomicBoolean();

        private Facet(OtlpExporter owner, SignalDescriptor<T> signal) {
            this.owner = owner;
            this.signal = signal;
        }

        @Override
        public CompletionStage<DeliveryResult> consume(
                ConsumeContext context, Payload<T> data) {
            return owner.export(context, data);
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            owner.start();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<FlushReport> flush(FlushContext context) {
            return owner.flush(context.deadline());
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            if (!released.compareAndSet(false, true)) {
                return CompletableFuture.completedFuture(null);
            }
            return owner.releaseFacet(context.deadline());
        }

        @Override
        public String toString() {
            return "OtlpExporter(" + signal.signalName() + ")";
        }
    }
}
