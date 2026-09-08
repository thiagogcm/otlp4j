package dev.nthings.otlp4j.samples;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Pipeline;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Primary in-process pipeline example.
///
/// The application owns the pipeline lifecycle and submits immutable model batches directly. The
/// OTLP exporter is the only transport-specific part of the setup; no generated protobuf type or
/// codec API is needed by the application. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to point at an OTLP/gRPC
/// endpoint before running the example (the default is `localhost:4317`).
public final class EmbeddedPipelineDemo {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedPipelineDemo.class);
    private static final String ENDUSER_ID = "enduser.id";

    private EmbeddedPipelineDemo() {
    }

    public static void main(String[] args) {
        var endpoint = System.getenv().getOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317");
        if (!endpoint.contains("://")) {
            endpoint = "http://" + endpoint;
        }
        var exporterConfig = OtlpExporterConfig.builder(4317).endpoint(endpoint).build();

        try (var traces = Pipeline.builder(TRACES)
                .filter(batch -> batch.spanCount() > 0)
                .map(EmbeddedPipelineDemo::redactEnduserId)
                .to(OtlpExporterFactory.grpc(), exporterConfig)
                .build()) {
            traces.start();
            var batch = sampleTraces();
            var result = traces.submit(batch).toCompletableFuture().join();
            log.info("submitted {} spans to {}: {}", batch.spanCount(), endpoint, result);
            traces.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
        }
    }

    static TracesData redactEnduserId(TracesData traces) {
        return new TracesData(traces.resourceSpans().stream()
                .map(resourceSpans -> new TracesData.ResourceSpans(
                        resourceSpans.resource(),
                        resourceSpans.schemaUrl(),
                        resourceSpans.scopeSpans().stream()
                                .map(scopeSpans -> new TracesData.ScopeSpans(
                                        scopeSpans.scope(),
                                        scopeSpans.schemaUrl(),
                                        scopeSpans.spans().stream()
                                                .map(span -> span.attributes().contains(ENDUSER_ID)
                                                        ? span.toBuilder()
                                                                .attributes(span.attributes().with(ENDUSER_ID, "***"))
                                                                .build()
                                                        : span)
                                                .toList()))
                                .toList()))
                .toList());
    }

    static TracesData sampleTraces() {
        var resource = Resource.of(Attributes.builder().put("service.name", "otlp4j-demo").build());
        var scope = InstrumentationScope.of("otlp4j-demo", "1.0.0");
        var span = Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name("GET /checkout")
                .kind(Span.Kind.SERVER)
                .startEpochNanos(1_000L)
                .endEpochNanos(2_000L)
                .attributes(Attributes.builder()
                        .put("http.request.method", "GET")
                        .put(ENDUSER_ID, AttributeValue.of("alice@example.com"))
                        .build())
                .build();
        return TracesData.of(resource, scope, List.of(span));
    }
}
