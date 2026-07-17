package dev.nthings.otlp4j.samples;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.components.connector.SpanCountConnectorConfig;
import dev.nthings.otlp4j.components.connector.SpanCountConnectorFactory;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.service.GatewayRuntime;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.OtlpExporter;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import dev.nthings.otlp4j.transport.OtlpReceiverFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// End-to-end demo: five spans flow through a service-graph gateway (enrich, redact, filter,
/// drop-empty, batch, fan out to an OTLP exporter and a span-count connector) to a backend gateway. The
/// batch stage and the span-count connector are built-in components from `otlp4j-components`; the four
/// domain transforms stay inline because they read the domain the routing/filter match engine does not.
/// Both the
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
                + "filter SERVER spans -> drop-empty-traces processor -> batch processor -> "
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
            try (var client = OtlpExporter.grpcBuilder().setEndpoint("localhost", gatewayPort).build()) {
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
    private static final ComponentId BATCH = ComponentId.parse("batch");
    private static final ComponentId TRACES_OUT = ComponentId.parse("traces-out");
    private static final ComponentId SPAN_COUNT = ComponentId.parse("spancount");
    private static final ComponentId METRICS_OUT = ComponentId.parse("metrics-out");

    /// Builds the backend graph: an OTLP/gRPC receiver on an ephemeral port feeding one capturing
    /// exporter, built inline through [ExporterFactory#of], that records the traces and metrics batches
    /// it terminates.
    private static GatewayRuntime buildBackend(
            AtomicReference<TracesData> backendTraces, List<Metric> backendMetrics) {
        var capturing = ExporterFactory.of("capture", (ctx, data, provenance) -> {
            var batch = data.view(PayloadView.DOMAIN_IMMUTABLE);
            if (batch instanceof TracesData traces) {
                backendTraces.set(traces);
            } else if (batch instanceof MetricsData metrics) {
                backendMetrics.addAll(metrics.metrics());
            }
            return DeliveryResult.acceptedStage(provenance);
        });
        var gw = Gateway.builder();
        var in = gw.receiver(BACKEND_RECV, OtlpReceiverFactory.grpc(), ephemeralReceiver());
        var sink = gw.exporter(BACKEND_SINK, capturing);
        gw.pipeline(PipelineId.of(TRACES), p -> p.from(in).to(sink));
        gw.pipeline(PipelineId.of(METRICS), p -> p.from(in).to(sink));
        return gw.build();
    }

    /// Builds the gateway graph: an OTLP/gRPC receiver, the four inline domain-transform processors
    /// (each built through [ProcessorFactory#of]) whose transforms read the domain the match engine
    /// cannot express (resource mutation, span-attribute redaction, span-kind filtering, empty-batch
    /// drop), the built-in [BatchProcessorFactory] coalescing what survives into one batch, and a
    /// fan-out to the OTLP exporter forwarding traces to the backend and the built-in
    /// [SpanCountConnectorFactory] deriving a metric that its own metrics pipeline exports to the backend
    /// too.
    private static GatewayRuntime buildGateway(int backendPort) {
        var gw = Gateway.builder();
        var in = gw.receiver(GATEWAY_RECV, OtlpReceiverFactory.grpc(), ephemeralReceiver());
        var enrich = gw.processor(ENRICH, ProcessorFactory.of("enrich", TRACES, enrichResource(), Payloads.DOMAIN));
        var redact = gw.processor(REDACT, ProcessorFactory.of("redact", TRACES, redactEnduserId(), Payloads.DOMAIN));
        var filter = gw.processor(FILTER, ProcessorFactory.of("filter", TRACES, keepServerSpans(), Payloads.DOMAIN));
        var dropEmpty =
                gw.processor(DROP_EMPTY, ProcessorFactory.of("drop-empty", TRACES, dropEmptyTraces(), Payloads.DOMAIN));
        var batch = gw.processor(BATCH, new BatchProcessorFactory(), batchConfig());
        var tracesOut = gw.exporter(TRACES_OUT, OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(backendPort));
        var spanCount = gw.connector(SPAN_COUNT, new SpanCountConnectorFactory(), SpanCountConnectorConfig.defaults());
        var metricsOut = gw.exporter(METRICS_OUT, OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(backendPort));
        gw.pipeline(PipelineId.of(TRACES),
                p -> p.from(in).via(enrich, redact, filter, dropEmpty, batch).to(tracesOut, spanCount));
        gw.pipeline(PipelineId.of(METRICS), p -> p.from(spanCount).to(metricsOut));
        return gw.build();
    }

    /// The batch stage's config: a short flush timeout so the single surviving batch coalesces and
    /// flushes promptly during the run rather than only on shutdown, keeping the whole surviving batch
    /// in one unit (no send-batch cap) so the span-count connector observes one count of three.
    private static BatchProcessorConfig batchConfig() {
        return new BatchProcessorConfig(
                new BatchPolicy(8192, 0, 0, Duration.ofMillis(200), List.of()),
                new DeliveryQueuePolicy(1024, 1, OverflowPolicy.BLOCK, false),
                Duration.ofSeconds(5));
    }

    private static OtlpReceiverConfig ephemeralReceiver() {
        return OtlpReceiverConfig.defaults(OtlpReceiverFactory.grpc().defaultPort()).withEphemeralPort();
    }

    private static int boundPort(GatewayRuntime runtime, ComponentId receiverId) {
        return runtime.boundPort(receiverId)
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
}
