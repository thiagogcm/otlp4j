# OpenTelemetry Java SDK integration

Add `otlp4j-opentelemetry` when OpenTelemetry Java instrumentation should feed an otlp4j input directly. The adapters implement the SDK's exporter interfaces and snapshot SDK data into immutable domain batches. This avoids a local encode, network, and decode hop.

The bridge targets the SDK version pinned by the build, currently `1.66.0`. The model, core, service, and transport do not depend on the official SDK. Continue using the SDK for instrumentation, propagation, sampling, and metric aggregation.

## Select an adapter

| Entry point | Input type | SDK interface |
| --- | --- | --- |
| `SdkExporters.spans(input)` | `TelemetryInput<TracesData>` | `SpanExporter` |
| `SdkExporters.metrics(input)` | `TelemetryInput<MetricsData>` | `MetricExporter` |
| `SdkExporters.logs(input)` | `TelemetryInput<LogsData>` | `LogRecordExporter` |

There is no profiles adapter in this bridge.

## Connect a trace provider

The following code sends a completed SDK span through an otlp4j trace pipeline to a running OTLP/gRPC destination on localhost. It closes the provider before the pipeline.

```java
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.opentelemetry.SdkExporters;
import dev.nthings.otlp4j.service.Pipeline;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

try (var traces = Pipeline.builder(TRACES)
        .to(OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(4317))
        .build()) {
    traces.start();
    try (var provider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(SdkExporters.spans(traces)))
            .build()) {
        var span = provider.get("checkout").spanBuilder("checkout").startSpan();
        span.end();
    }
}
```

`SimpleSpanProcessor` keeps the example small. Select SDK processing and export scheduling according to your application requirements. The bridge itself adds no buffer; add [explicit otlp4j batching](pipelines.md#add-buffering-deliberately) when needed.

## Configure metric temporality

`SdkExporters.metrics(input)` requests cumulative aggregation temporality. Use the overload accepting the SDK's `AggregationTemporalitySelector` when a different policy is required:

```java
import dev.nthings.otlp4j.opentelemetry.SdkExporters;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;

// metricsInput is a running TelemetryInput<MetricsData>.
var exporter = SdkExporters.metrics(
        metricsInput, AggregationTemporalitySelector.deltaPreferred());
```

The SDK performs the aggregation. The adapter reports the temporality selector and maps the data supplied by the SDK; it does not convert cumulative data to delta after export.

## Ownership and results

The adapter borrows its `TelemetryInput`. It neither starts nor closes that input. Start the runtime before instrumentation exports and keep it running until provider shutdown completes.

| Adapter event | Behavior |
| --- | --- |
| Export returns `Accepted` or `Partial` | SDK export reports success; partial delivery remains terminal |
| Export returns `Rejected` | SDK export reports failure |
| Export throws or completes exceptionally | SDK export reports failure |
| `flush()` | Flush the input with a ten-second budget; fail if the report is unsuccessful |
| `shutdown()` | Stop accepting adapter exports, wait for outstanding submissions, and flush within one ten-second budget |

Shutdown is idempotent and preserves its result. It fails if pending submissions do not settle within the budget. A custom input must complete its submission stages and implement meaningful flush behavior. For buffered pipelines, later losses are visible through input flush even when SDK export previously reported success.

The direct adapter callback maps partial success to SDK success to avoid replaying accepted items. See [Delivery and lifecycle](delivery.md) for the detailed domain outcomes.

## Mapping boundaries

Adapters preserve the exporter data exposed by the targeted SDK, including resource and scope grouping, schema URLs, span events and links, metric points and exemplars, and log bodies. Mutable collections and byte values are copied before submission; span and link remote-context bits are preserved.

Some OTLP fields are not exposed by those SDK exporter interfaces:

| OTLP field                                  | Emitted model value |
| ------------------------------------------- | ------------------- |
| Metric point flags                          | Zero                |
| Exponential-histogram zero threshold        | Zero                |
| Resource and scope dropped-attribute counts | Zero                |
| Metric metadata                             | Empty attributes    |

These defaults describe SDK-to-model adaptation. OTLP network payloads use the separate [codec boundary](architecture.md#payloads-and-wire-fidelity).
