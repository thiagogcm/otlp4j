package dev.nthings.otlp4j.samples;

import dev.nthings.otlp4j.connector.CountConnector;
import dev.nthings.otlp4j.exporter.OtlpGrpcExporter;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.Pipeline;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import dev.nthings.otlp4j.processor.Processors;
import dev.nthings.otlp4j.receiver.OtlpReceiver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// End-to-end demo compiled only against `dev.nthings.otlp4j.api`.
///
/// The runtime transport is supplied through the SPI. The demo sends five spans through a gateway,
/// enriches and filters them, exports the surviving traces, and derives a span-count metric.
public final class OtlpE2eDemo {

    private static final Logger log = LoggerFactory.getLogger(OtlpE2eDemo.class);

    private OtlpE2eDemo() {}

    /// The observable outcome of one demo run — what the `backend` receiver ended up with.
    public record Result(int spansAtBackend, long derivedSpanCount, String enrichedEnvironment) {}

    public static void main(String[] args) throws Exception {
        var result = run();
        log.info("=== otlp4j end-to-end demo ===");
        log.info("Client sent 5 spans (3 SERVER, 2 INTERNAL) to the gateway.");
        log.info("Gateway pipeline: enrich resource attribute -> filter SERVER spans -> "
                + "fan out to exporter + CountConnector.");
        log.info("Backend received:");
        log.info("  filtered spans          : {} (expected 3)", result.spansAtBackend());
        log.info("  derived span-count metric: {} (expected 3)", result.derivedSpanCount());
        log.info("  enriched resource attr   : deployment.environment={} (added by the processor)",
                result.enrichedEnvironment());
        log.info("All telemetry crossed two real OTLP/gRPC hops; this class never touched "
                + "a proto or gRPC type.");
    }

    /// Runs the full pipeline once and returns what reached the backend. Servers use ephemeral
    /// ports and are torn down before returning; the calls are synchronous, so no waiting is needed.
    public static Result run() throws Exception {
        var backendTraces = new AtomicReference<TraceData>();
        var backendMetrics = Collections.synchronizedList(new ArrayList<Metric>());

        OtlpReceiver backend = null;
        OtlpGrpcExporter backendExporter = null;
        OtlpReceiver gateway = null;
        try {
            // --- Backend: the final destination, capturing whatever survives the pipeline. ------
            backend = OtlpReceiver.builder()
                    .traceHandler(traces -> {
                        backendTraces.set(traces);
                        return ExportResult.success();
                    })
                    .metricsHandler(metrics -> {
                        backendMetrics.addAll(metrics.metrics());
                        return ExportResult.success();
                    })
                    .build()
                    .start(0);
            log.info("Backend receiver started on port {}.", backend.port());
            backendExporter =
                    OtlpGrpcExporter.builder().endpoint("localhost", backend.port()).build();

            // --- Gateway: receive -> [enrich -> filter] -> fan out to exporter + count connector.
            var toBackend = backendExporter;
            var spanCounter = new CountConnector(toBackend);
            var fanOut = new TelemetryConsumer() {
                @Override
                public ExportResult consumeTraces(TraceData traces) {
                    return toBackend.consumeTraces(traces).and(spanCounter.consumeTraces(traces));
                }
            };
            var gatewayPipeline = Pipeline.builder()
                    .process(Processors.setResourceAttribute(
                            "deployment.environment", AttributeValue.of("demo")))
                    .process(Processors.filterSpans(span -> span.kind() == Span.Kind.SERVER))
                    .into(fanOut);
            gateway = OtlpReceiver.builder().consumer(gatewayPipeline).build().start(0);
            log.info("Gateway receiver started on port {}.", gateway.port());

            // --- Client: export a mixed batch of spans to the gateway. --------------------------
            try (var client =
                    OtlpGrpcExporter.builder().endpoint("localhost", gateway.port()).build()) {
                client.consumeTraces(sampleTraces());
                log.info("Client exported sample trace batch to the gateway.");
            }
        } finally {
            // The export above is synchronous, so the backend is fully populated by now.
            if (backendExporter != null) {
                backendExporter.close();
            }
            if (gateway != null) {
                gateway.shutdownNow().awaitTermination(Duration.ofSeconds(5));
            }
            if (backend != null) {
                backend.shutdownNow().awaitTermination(Duration.ofSeconds(5));
            }
        }

        var atBackend = backendTraces.get();
        var spans = atBackend == null ? 0 : atBackend.spans().size();
        var environment = atBackend == null
                ? "<none>"
                : atBackend.resourceSpans().get(0).resource().attributes()
                        .get("deployment.environment")
                        .map(value -> value instanceof AttributeValue.StringValue s ? s.value() : "?")
                        .orElse("<none>");
        var derivedCount = backendMetrics.stream()
                .filter(metric -> metric.name().equals("otlp4j.connector.span.count"))
                .findFirst()
                .map(OtlpE2eDemo::longValueOf)
                .orElse(-1L);
        return new Result(spans, derivedCount, environment);
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
        var resource = new Resource(
                Attributes.builder().put("service.name", "checkout").build(), 0);
        var scope = new InstrumentationScope("otlp4j-demo", "1.0.0", Attributes.empty(), 0);
        var spans = List.of(
                span("GET /cart", Span.Kind.SERVER),
                span("GET /checkout", Span.Kind.SERVER),
                span("POST /pay", Span.Kind.SERVER),
                span("db.query", Span.Kind.INTERNAL),
                span("cache.lookup", Span.Kind.INTERNAL));
        return new TraceData(List.of(new TraceData.ResourceSpans(
                resource, "", List.of(new TraceData.ScopeSpans(scope, "", spans)))));
    }

    private static Span span(String name, Span.Kind kind) {
        return Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name(name)
                .kind(kind)
                .startEpochNanos(1_000L)
                .endEpochNanos(2_000L)
                .build();
    }
}
