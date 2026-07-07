package dev.nthings.otlp4j.samples;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.ModelBatchPayload;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.samples.support.CountConnectorConfig;
import dev.nthings.otlp4j.samples.support.CountConnectorFactory;
import dev.nthings.otlp4j.samples.support.FunctionProcessorConfig;
import dev.nthings.otlp4j.samples.support.FunctionProcessorFactory;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.transport.common.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.common.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcExporter;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcExporterFactory;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcReceiverFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// End-to-end demo: five spans flow through a service-graph gateway (enrich, redact, filter,
/// drop-empty, fan out to an OTLP exporter and a span-count connector) to a backend gateway. Both the
/// gateway and the backend are single [dev.nthings.otlp4j.service.config.GatewayConfig] graphs booted
/// by a [GatewayRuntime]; telemetry still crosses two real OTLP/gRPC hops, and this class never
/// touches a proto or gRPC wire type.
public final class OtlpE2eDemo {

    private static final Logger log = LoggerFactory.getLogger(OtlpE2eDemo.class);

    private static final String ENDUSER_ID = "enduser.id";
    private static final String REDACTED = "***";
    private static final String DERIVED_METRIC = "otlp4j.connector.span.count";

    private OtlpE2eDemo() {
    }

    /// The observable outcome of one demo run - what the backend receiver ended
    /// up with.
    public record Result(
            int spansAtBackend, long derivedSpanCount, String enrichedEnvironment, String redactedEnduserId) {
    }

    public static void main(String[] args) throws Exception {
        var result = run();
        log.info("=== otlp4j end-to-end demo ===");
        log.info("Client sent 5 spans (3 SERVER, 2 INTERNAL) to the gateway.");
        log.info("Gateway pipeline: enrich resource attribute -> redact enduser.id span attribute -> "
                + "filter SERVER spans -> drop-empty-traces processor -> "
                + "fan out to exporter + span count connector.");
        log.info("Backend received:");
        log.info("  filtered spans          : {} (expected 3)", result.spansAtBackend());
        log.info("  derived span-count metric: {} (expected 3)", result.derivedSpanCount());
        log.info("  enriched resource attr   : deployment.environment={} (added by the processor)",
                result.enrichedEnvironment());
        log.info("  redacted span attribute  : enduser.id={} (masked by the processor)",
                result.redactedEnduserId());
        log.info("All telemetry crossed two real OTLP/gRPC hops; this class never touched "
                + "a proto or gRPC type.");
    }

    /// Runs the full pipeline once and returns what reached the backend.
    public static Result run() throws Exception {
        var backendTraces = new AtomicReference<TracesData>();
        var backendMetrics = Collections.synchronizedList(new ArrayList<Metric>());

        GatewayRuntime backend = null;
        GatewayRuntime gateway = null;
        try {
            // --- Backend: the final destination, a graph that captures whatever survives the gateway
            // pipeline into memory. One OTLP/gRPC receiver feeds a capturing exporter on both signals.
            backend = buildBackend(backendTraces, backendMetrics);
            backend.start();
            var backendPort = boundPort(backend, BACKEND_RECV);
            log.info("Backend receiver started on port {}.", backendPort);

            // --- Gateway: receive -> enrich -> redact -> filter -> drop-empty -> fan out to the OTLP
            // exporter (forwarding traces to the backend) + the span-count connector (deriving a
            // metric into the metrics pipeline, itself exported to the backend).
            gateway = buildGateway(backendPort);
            gateway.start();
            var gatewayPort = boundPort(gateway, GATEWAY_RECV);
            log.info("Gateway receiver started on port {}.", gatewayPort);

            // --- Client: export a mixed batch of spans to the gateway and block until it is handled.
            try (var client = OtlpGrpcExporter.to("localhost", gatewayPort)) {
                client.export(SignalDescriptor.TRACES, sampleTraces()).toCompletableFuture().join();
                log.info("Client exported sample trace batch to the gateway.");
            }
        } finally {
            if (gateway != null) {
                gateway.close();
            }
            if (backend != null) {
                backend.close();
            }
        }

        var atBackend = backendTraces.get();
        var spans = atBackend == null ? 0 : atBackend.spanCount();
        var environment = atBackend == null
                ? "<none>"
                : extractEnvironment(atBackend);
        var redactedEnduserId = atBackend == null
                ? "<none>"
                : extractEnduserId(atBackend);
        var derivedCount = backendMetrics.stream()
                .filter(metric -> metric.name().equals(DERIVED_METRIC))
                .findFirst()
                .map(OtlpE2eDemo::longValueOf)
                .orElse(-1L);
        return new Result(spans, derivedCount, environment, redactedEnduserId);
    }

    // ---- graph assembly --------------------------------------------------------------------------

    private static final ComponentId BACKEND_RECV = ComponentId.parse("in");
    private static final ComponentId BACKEND_SINK = ComponentId.parse("capture");
    private static final ComponentId GATEWAY_RECV = ComponentId.parse("in");
    private static final ComponentId ENRICH = ComponentId.parse("enrich");
    private static final ComponentId REDACT = ComponentId.parse("redact");
    private static final ComponentId FILTER = ComponentId.parse("filter");
    private static final ComponentId DROP_EMPTY = ComponentId.parse("drop-empty");
    private static final ComponentId TRACES_OUT = ComponentId.parse("traces-out");
    private static final ComponentId SPAN_COUNT = ComponentId.parse("spancount");
    private static final ComponentId METRICS_OUT = ComponentId.parse("metrics-out");

    /// Builds the backend graph: an OTLP/gRPC receiver on an ephemeral port feeding one capturing
    /// exporter that records the traces and metrics batches it terminates.
    private static GatewayRuntime buildBackend(
            AtomicReference<TracesData> backendTraces, List<Metric> backendMetrics) {
        Consumer<TelemetryPayload> capture = payload -> {
            var batch = ((ModelBatchPayload<?>) payload).batch();
            if (batch instanceof TracesData traces) {
                backendTraces.set(traces);
            } else if (batch instanceof MetricsData metrics) {
                backendMetrics.addAll(metrics.metrics());
            }
        };
        var capturing = new CapturingExporterFactory(capture);
        return Gateway.builder()
                .receiver(BACKEND_RECV, new OtlpGrpcReceiverFactory(), ephemeralReceiver())
                .exporter(BACKEND_SINK, capturing, EmptyConfig.INSTANCE)
                .pipeline(PipelineId.of(TRACES),
                        new PipelineConfig(List.of(BACKEND_RECV), List.of(), List.of(BACKEND_SINK)))
                .pipeline(PipelineId.of(METRICS),
                        new PipelineConfig(List.of(BACKEND_RECV), List.of(), List.of(BACKEND_SINK)))
                .build();
    }

    /// Builds the gateway graph: an OTLP/gRPC receiver, the four function processors, and a fan-out to
    /// the OTLP exporter forwarding traces to the backend and the span-count connector deriving a
    /// metric that its own metrics pipeline exports to the backend too.
    private static GatewayRuntime buildGateway(int backendPort) {
        var enrich = new FunctionProcessorFactory<>("enrich", TRACES, enrichResource());
        var redact = new FunctionProcessorFactory<>("redact", TRACES, redactEnduserId());
        var filter = new FunctionProcessorFactory<>("filter", TRACES, keepServerSpans());
        var dropEmpty = new FunctionProcessorFactory<>("drop-empty", TRACES, dropEmptyTraces());
        return Gateway.builder()
                .receiver(GATEWAY_RECV, new OtlpGrpcReceiverFactory(), ephemeralReceiver())
                .processor(ENRICH, enrich, FunctionProcessorConfig.defaults())
                .processor(REDACT, redact, FunctionProcessorConfig.defaults())
                .processor(FILTER, filter, FunctionProcessorConfig.defaults())
                .processor(DROP_EMPTY, dropEmpty, FunctionProcessorConfig.defaults())
                .exporter(TRACES_OUT, new OtlpGrpcExporterFactory(), OtlpExporterConfig.defaults(backendPort))
                .connector(SPAN_COUNT, new CountConnectorFactory(), CountConnectorConfig.defaults())
                .exporter(METRICS_OUT, new OtlpGrpcExporterFactory(), OtlpExporterConfig.defaults(backendPort))
                .pipeline(PipelineId.of(TRACES), new PipelineConfig(
                        List.of(GATEWAY_RECV),
                        List.of(ENRICH, REDACT, FILTER, DROP_EMPTY),
                        List.of(TRACES_OUT, SPAN_COUNT)))
                .pipeline(PipelineId.of(METRICS), new PipelineConfig(
                        List.of(SPAN_COUNT), List.of(), List.of(METRICS_OUT)))
                .build();
    }

    private static OtlpReceiverConfig ephemeralReceiver() {
        return OtlpReceiverConfig.defaults(OtlpGrpcReceiverFactory.DEFAULT_PORT).withEphemeralPort();
    }

    private static int boundPort(GatewayRuntime runtime, ComponentId receiverId) {
        return runtime.component(receiverId)
                .filter(NetworkBound.class::isInstance)
                .map(NetworkBound.class::cast)
                .map(NetworkBound::port)
                .orElseThrow(() -> new IllegalStateException("receiver " + receiverId + " has no bound port"));
    }

    // ---- the pipeline transforms, as pure batch functions ----------------------------------------

    /// Adds `deployment.environment=demo` to every resource in the batch.
    private static java.util.function.UnaryOperator<@Nullable TracesData> enrichResource() {
        return traces -> mapResource(traces, resource -> resource.withAttribute(
                "deployment.environment", AttributeValue.of("demo")));
    }

    /// Masks the sensitive `enduser.id` span attribute wherever it is present.
    private static java.util.function.UnaryOperator<@Nullable TracesData> redactEnduserId() {
        return traces -> mapSpans(traces, span -> span.attributes().contains(ENDUSER_ID)
                ? span.toBuilder()
                        .attributes(span.attributes().with(ENDUSER_ID, REDACTED))
                        .build()
                : span);
    }

    /// Keeps only `SERVER` spans, pruning scopes and resources left empty.
    private static java.util.function.UnaryOperator<@Nullable TracesData> keepServerSpans() {
        return traces -> keepSpansWhere(traces, span -> span.kind() == Span.Kind.SERVER);
    }

    /// Drops the whole batch once the filter has emptied it, forwarding nothing downstream.
    private static java.util.function.UnaryOperator<@Nullable TracesData> dropEmptyTraces() {
        return traces -> traces.spanCount() == 0 ? null : traces;
    }

    private static TracesData mapResource(
            TracesData traces, java.util.function.UnaryOperator<Resource> mapper) {
        return new TracesData(traces.resourceSpans().stream()
                .map(rs -> new TracesData.ResourceSpans(
                        mapper.apply(rs.resource()), rs.schemaUrl(), rs.scopeSpans()))
                .toList());
    }

    private static TracesData mapSpans(TracesData traces, java.util.function.UnaryOperator<Span> mapper) {
        return new TracesData(traces.resourceSpans().stream()
                .map(rs -> new TracesData.ResourceSpans(
                        rs.resource(),
                        rs.schemaUrl(),
                        rs.scopeSpans().stream()
                                .map(ss -> new TracesData.ScopeSpans(
                                        ss.scope(),
                                        ss.schemaUrl(),
                                        ss.spans().stream().map(mapper).toList()))
                                .toList()))
                .toList());
    }

    private static TracesData keepSpansWhere(TracesData traces, java.util.function.Predicate<Span> keep) {
        var resources = new ArrayList<TracesData.ResourceSpans>();
        for (var rs : traces.resourceSpans()) {
            var scopes = new ArrayList<TracesData.ScopeSpans>();
            for (var ss : rs.scopeSpans()) {
                var kept = ss.spans().stream().filter(keep).toList();
                if (!kept.isEmpty()) {
                    scopes.add(new TracesData.ScopeSpans(ss.scope(), ss.schemaUrl(), kept));
                }
            }
            if (!scopes.isEmpty()) {
                resources.add(new TracesData.ResourceSpans(rs.resource(), rs.schemaUrl(), scopes));
            }
        }
        return new TracesData(resources);
    }

    // ---- backend readback ------------------------------------------------------------------------

    private static String extractEnduserId(TracesData traces) {
        if (traces.spanCount() == 0) {
            return "<none>";
        }
        var value = traces.spans().get(0).attributes().get(ENDUSER_ID);
        return value instanceof AttributeValue.StringValue s ? s.value() : "<none>";
    }

    private static String extractEnvironment(TracesData traces) {
        if (traces.resourceSpans().isEmpty()) {
            return "<none>";
        }
        var attrs = traces.resourceSpans().get(0).resource().attributes();
        var value = attrs.get("deployment.environment");
        return value instanceof AttributeValue.StringValue s ? s.value() : "<none>";
    }

    private static long longValueOf(Metric metric) {
        if (metric.data() instanceof Metric.Sum sum
                && !sum.points().isEmpty()
                && sum.points().get(0).value() instanceof NumberPoint.LongValue longValue) {
            return longValue.value();
        }
        return -1L;
    }

    private static TracesData sampleTraces() {
        var resource = Resource.of(Attributes.builder().put("service.name", "checkout").build());
        var scope = InstrumentationScope.of("otlp4j-demo", "1.0.0");
        var spans = List.of(
                span("GET /cart", Span.Kind.SERVER),
                span("GET /checkout", Span.Kind.SERVER),
                span("POST /pay", Span.Kind.SERVER),
                span("db.query", Span.Kind.INTERNAL),
                span("cache.lookup", Span.Kind.INTERNAL));
        return TracesData.of(resource, scope, spans);
    }

    private static Span span(String name, Span.Kind kind) {
        return Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name(name)
                .kind(kind)
                .startEpochNanos(1_000L)
                .endEpochNanos(2_000L)
                // Sensitive attribute the gateway redacts before forwarding.
                .attributes(Attributes.builder().put(ENDUSER_ID, "alice@example.com").build())
                .build();
    }

    // ---- the in-memory backend exporter ----------------------------------------------------------

    /// The (empty) config shared by the demo's own components that carry no configuration.
    private record EmptyConfig() implements ComponentConfig {
        static final EmptyConfig INSTANCE = new EmptyConfig();
    }

    /// A terminal [ExporterFactory] whose exporter records every batch it receives into the demo's
    /// backend capture sink, standing in for a real backend store.
    private static final class CapturingExporterFactory implements ExporterFactory<EmptyConfig> {

        private final Consumer<TelemetryPayload> sink;

        CapturingExporterFactory(Consumer<TelemetryPayload> sink) {
            this.sink = sink;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.EXPORTER;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.DEVELOPMENT;
        }

        @Override
        public EmptyConfig defaultConfig() {
            return EmptyConfig.INSTANCE;
        }

        @Override
        public String type() {
            return "capture";
        }

        @Override
        public <T extends TelemetryPayload> Exporter<T> create(
                CreateContext ctx, EmptyConfig config, SignalDescriptor<T> signal) {
            return new CapturingExporter<>(new Provenance(ctx.id(), ComponentKind.EXPORTER, signal), sink);
        }
    }

    private static final class CapturingExporter<T extends TelemetryPayload> implements Exporter<T> {

        private final Provenance provenance;
        private final Consumer<TelemetryPayload> sink;

        CapturingExporter(Provenance provenance, Consumer<TelemetryPayload> sink) {
            this.provenance = provenance;
            this.sink = sink;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, T data) {
            sink.accept(data);
            return DeliveryResult.acceptedStage(provenance);
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
