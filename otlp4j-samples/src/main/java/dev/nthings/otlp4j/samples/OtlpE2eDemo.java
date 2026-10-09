package dev.nthings.otlp4j.samples;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.components.connector.SpanCountConnectorConfig;
import dev.nthings.otlp4j.components.connector.SpanCountConnectorFactory;
import dev.nthings.otlp4j.components.routing.FilterConfig;
import dev.nthings.otlp4j.components.routing.FilterProcessorFactory;
import dev.nthings.otlp4j.components.routing.MatchRule;
import dev.nthings.otlp4j.components.routing.MatchSpec;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
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
import dev.nthings.otlp4j.transport.OtlpExporter;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.OtlpReceiverFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// End-to-end demo: one request carrying spans from two services flows through a service-graph gateway
/// (drop one service's resource, enrich, redact, filter, drop-empty, batch, fan out to an OTLP exporter
/// and a span-count connector) to a backend gateway. The resource filter, the batch stage, and the
/// span-count connector are built-in components from `otlp4j-components`; the four domain transforms
/// stay inline through the typed `ProcessorFactory.map` and `filter` helpers.
/// Both the
/// gateway and the backend are single [dev.nthings.otlp4j.service.config.GatewayConfig] graphs booted
/// by a [GatewayRuntime]; telemetry still crosses two real OTLP/gRPC hops, and this class never
/// touches a proto or gRPC wire type.
public final class OtlpE2eDemo {

    private static final Logger log = LoggerFactory.getLogger(OtlpE2eDemo.class);

    private static final String ENDUSER_ID = "enduser.id";
    private static final String REDACTED = "***";
    private static final String DERIVED_METRIC = "otlp4j.connector.span.count";
    private static final String SERVICE_NAME = "service.name";
    private static final String MONITOR_SERVICE = "synthetic-monitor";

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
        log.info("Client sent one request to the gateway: 5 checkout spans (3 SERVER, 2 INTERNAL) and "
                + "2 SERVER spans from {}.", MONITOR_SERVICE);
        log.info("Gateway pipeline: drop the {} resource -> enrich resource attribute -> "
                + "redact enduser.id span attribute -> filter SERVER spans -> drop-empty-traces processor -> "
                + "batch processor -> fan out to exporter + span count connector.", MONITOR_SERVICE);
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

            // --- Gateway: receive -> drop the monitor's resource -> enrich -> redact -> filter ->
            // drop-empty -> batch -> fan out to the OTLP exporter (forwarding traces to the backend) +
            // the span-count connector (deriving a metric into the metrics pipeline, itself exported
            // to the backend).
            gateway = buildGateway(backendPort);
            gateway.start();
            var gatewayPort = boundPort(gateway, GATEWAY_RECV);
            log.info("Gateway receiver started on port {}.", gatewayPort);

            // --- Client: export a mixed batch of spans to the gateway and block until it is handled.
            try (var client = OtlpExporter.grpcBuilder().endpoint("localhost", gatewayPort).build()) {
                client.start();
                client.export(TRACES, sampleTraces()).toCompletableFuture().join();
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
    private static final ComponentId DROP_MONITOR = ComponentId.parse("drop-monitor");
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
            var batch = data.data();
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

    /// Builds the gateway graph: an OTLP/gRPC receiver, the built-in [FilterProcessorFactory] dropping
    /// the monitor's resource group from the mixed request, the four inline domain-transform processors
    /// (built through [ProcessorFactory#map] and [ProcessorFactory#filter]) whose transforms read the domain the match engine
    /// cannot express (resource mutation, span-attribute redaction, span-kind filtering, empty-batch
    /// drop), the built-in [BatchProcessorFactory] coalescing what survives into one batch, and a
    /// fan-out to the OTLP exporter forwarding traces to the backend and the built-in
    /// [SpanCountConnectorFactory] deriving a metric that its own metrics pipeline exports to the backend
    /// too.
    private static GatewayRuntime buildGateway(int backendPort) {
        var gw = Gateway.builder();
        var in = gw.receiver(GATEWAY_RECV, OtlpReceiverFactory.grpc(), ephemeralReceiver());
        var dropMonitor = gw.processor(DROP_MONITOR, new FilterProcessorFactory(), new FilterConfig(
                MatchSpec.all(),
                MatchSpec.of(MatchRule.builder().resourceAttribute(SERVICE_NAME, MONITOR_SERVICE).build())));
        var enrich = gw.processor(ENRICH, ProcessorFactory.map("enrich", TRACES, enrichResource()));
        var redact = gw.processor(REDACT, ProcessorFactory.map("redact", TRACES, redactEnduserId()));
        var filter = gw.processor(FILTER, ProcessorFactory.map("filter", TRACES, keepServerSpans()));
        var dropEmpty = gw.processor(
                DROP_EMPTY, ProcessorFactory.filter("drop-empty", TRACES, traces -> traces.spanCount() > 0));
        var batch = gw.processor(BATCH, new BatchProcessorFactory(), batchConfig());
        var tracesOut = gw.exporter(TRACES_OUT, OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(backendPort));
        var spanCount = gw.connector(SPAN_COUNT, new SpanCountConnectorFactory(), SpanCountConnectorConfig.defaults());
        var metricsOut = gw.exporter(METRICS_OUT, OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(backendPort));
        gw.pipeline(PipelineId.of(TRACES),
                p -> p.from(in).via(dropMonitor, enrich, redact, filter, dropEmpty, batch)
                        .to(tracesOut, spanCount));
        gw.pipeline(PipelineId.of(METRICS), p -> p.from(spanCount).to(metricsOut));
        return gw.build();
    }

    /// The batch stage's config: a short flush timeout so the single surviving batch coalesces and
    /// flushes promptly during the run rather than only on shutdown, keeping the whole surviving batch
    /// in one unit (no send-batch cap) so the span-count connector observes one count of three.
    private static BatchProcessorConfig batchConfig() {
        return new BatchProcessorConfig(
                new BatchPolicy(8192, 0, 0, Duration.ofMillis(200), List.of()),
                new DeliveryQueuePolicy(1024, 1, OverflowPolicy.FAIL),
                Duration.ofSeconds(5),
                BatchProcessorConfig.DEFAULT_MAX_PENDING_SUBMISSIONS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES,
                BatchProcessorConfig.DEFAULT_MAX_PARTITIONS);
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
    private static UnaryOperator<TracesData> enrichResource() {
        return traces -> traces.mapResources(resource -> resource.withAttribute(
                "deployment.environment", AttributeValue.of("demo")));
    }

    /// Masks the sensitive `enduser.id` span attribute wherever it is present.
    private static UnaryOperator<TracesData> redactEnduserId() {
        return traces -> traces.mapSpans(span -> span.attributes().contains(ENDUSER_ID)
                ? span.toBuilder()
                        .attributes(span.attributes().with(ENDUSER_ID, REDACTED))
                        .build()
                : span);
    }

    /// Keeps only `SERVER` spans; scopes and resources left without spans are removed.
    private static UnaryOperator<TracesData> keepServerSpans() {
        return traces -> traces.filterSpans(span -> span.kind() == Span.Kind.SERVER);
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

    /// One request mixing two services: the checkout spans the gateway processes and a monitor's
    /// `SERVER` spans its resource filter drops.
    private static TracesData sampleTraces() {
        return new TracesData(List.of(
                resourceSpans("checkout",
                        span("GET /cart", Span.Kind.SERVER),
                        span("GET /checkout", Span.Kind.SERVER),
                        span("POST /pay", Span.Kind.SERVER),
                        span("db.query", Span.Kind.INTERNAL),
                        span("cache.lookup", Span.Kind.INTERNAL)),
                resourceSpans(MONITOR_SERVICE,
                        span("GET /health", Span.Kind.SERVER),
                        span("GET /ready", Span.Kind.SERVER))));
    }

    private static TracesData.ResourceSpans resourceSpans(String serviceName, Span... spans) {
        var resource = Resource.of(Attributes.builder().put(SERVICE_NAME, serviceName).build());
        var scope = InstrumentationScope.of("otlp4j-demo", "1.0.0");
        return new TracesData.ResourceSpans(
                resource, "", List.of(new TracesData.ScopeSpans(scope, "", List.of(spans))));
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
