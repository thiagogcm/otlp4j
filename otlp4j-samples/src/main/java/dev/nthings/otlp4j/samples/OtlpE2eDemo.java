package dev.nthings.otlp4j.samples;

import dev.nthings.otlp4j.connector.Connectors;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.processor.Transforms;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcExporter;
import dev.nthings.otlp4j.transport.grpc.OtlpGrpcReceiver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// End-to-end demo: five spans flow through a gateway pipeline (enrich, redact,
/// filter, fan out to the exporter and a span-count sink) to a backend. Register
/// the exporter with `owns(exporter)` so the subscription drains it on shutdown.
public final class OtlpE2eDemo {

    private static final Logger log = LoggerFactory.getLogger(OtlpE2eDemo.class);

    private static final String ENDUSER_ID = "enduser.id";
    private static final String REDACTED = "***";

    private OtlpE2eDemo() {
    }

    /// The observable outcome of one demo run — what the `backend` receiver ended
    /// up with.
    public record Result(
            int spansAtBackend, long derivedSpanCount, String enrichedEnvironment, String redactedEnduserId) {
    }

    public static void main(String[] args) throws Exception {
        var result = run();
        log.info("=== otlp4j end-to-end demo ===");
        log.info("Client sent 5 spans (3 SERVER, 2 INTERNAL) to the gateway.");
        log.info("Gateway pipeline: enrich resource attribute -> redact enduser.id span attribute -> "
                + "filter SERVER spans -> fan out to exporter + span count sink.");
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
        var backendTraces = new AtomicReference<TraceData>();
        var backendMetrics = Collections.synchronizedList(new ArrayList<Metric>());

        OtlpGrpcReceiver backend = null;
        OtlpGrpcExporter backendExporter = null;
        OtlpGrpcReceiver gateway = null;
        try {
            // --- Backend: the final destination, capturing whatever survives the pipeline.
            backend = OtlpGrpcReceiver.builder()
                    .ephemeralPort()
                    .onTraces(TraceSink.accepting(backendTraces::set))
                    .onMetrics(MetricSink.accepting(metrics -> backendMetrics.addAll(metrics.metrics())))
                    .build()
                    .start();
            log.info("Backend receiver started on port {}.", backend.port());
            backendExporter = OtlpGrpcExporter.to("localhost", backend.port());

            // --- Gateway: receive -> enrich -> filter -> fan out to exporter + count sink.
            gateway = OtlpGrpcReceiver.builder().ephemeralPort().build().start();
            log.info("Gateway receiver started on port {}.", gateway.port());

            var spanCounter = Connectors.spanCount(backendExporter.metrics());

            var subscription = Pipeline.from(gateway.traces())
                    .transform(Transforms.withTracesResourceAttribute(
                            "deployment.environment", AttributeValue.of("demo")))
                    // Redact a sensitive span attribute via the map helper.
                    .transform(Transforms.mapSpans(span -> span.attributes().contains(ENDUSER_ID)
                            ? span.toBuilder()
                                    .attributes(span.attributes().with(ENDUSER_ID, REDACTED))
                                    .build()
                            : span))
                    .transform(Transforms.keepSpansWhere(span -> span.kind() == Span.Kind.SERVER))
                    .filter(traces -> traces.spanCount() != 0)
                    .owns(backendExporter)
                    .branch()
                    .fanOut(backendExporter.traces())
                    .fanOut(spanCounter)
                    .join();

            // --- Client: export a mixed batch of spans to the gateway.
            try (var client = OtlpGrpcExporter.to("localhost", gateway.port())) {
                client.traces().consume(sampleTraces()).toCompletableFuture().join();
                log.info("Client exported sample trace batch to the gateway.");
            }
            subscription.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
        } finally {
            // Idempotent backstop: the subscription already drained the exporter via
            // owns().
            if (backendExporter != null) {
                backendExporter.close();
            }
            if (gateway != null) {
                gateway.shutdownNow().toCompletableFuture().join();
            }
            if (backend != null) {
                backend.shutdownNow().toCompletableFuture().join();
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
                .filter(metric -> metric.name().equals("otlp4j.connector.span.count"))
                .findFirst()
                .map(OtlpE2eDemo::longValueOf)
                .orElse(-1L);
        return new Result(spans, derivedCount, environment, redactedEnduserId);
    }

    private static String extractEnduserId(TraceData traces) {
        if (traces.spanCount() == 0) {
            return "<none>";
        }
        var value = traces.spans().get(0).attributes().get(ENDUSER_ID);
        return value instanceof AttributeValue.StringValue s ? s.value() : "<none>";
    }

    private static String extractEnvironment(TraceData traces) {
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

    private static TraceData sampleTraces() {
        var resource = Resource.of(Attributes.builder().put("service.name", "checkout").build());
        var scope = InstrumentationScope.of("otlp4j-demo", "1.0.0");
        var spans = List.of(
                span("GET /cart", Span.Kind.SERVER),
                span("GET /checkout", Span.Kind.SERVER),
                span("POST /pay", Span.Kind.SERVER),
                span("db.query", Span.Kind.INTERNAL),
                span("cache.lookup", Span.Kind.INTERNAL));
        return TraceData.of(resource, scope, spans);
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
