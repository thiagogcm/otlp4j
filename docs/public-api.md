# Public API

The application-facing API is intentionally small and typed. Use `Pipeline` for one in-process signal flow, `Gateway` for a named multi-component graph, and `OtlpExporter` or the transport factories for OTLP wire delivery. Generated protobuf and gRPC classes are not part of the application surface.

All exported packages are JPMS modules. Public reference types are non-null by default where the module declares `@NullMarked`; nullable values are annotated explicitly.

## The shortest path: `Pipeline`

`dev.nthings.otlp4j.service.Pipeline<T>` owns a typed graph for one signal. Its builder creates an application input, accepts synchronous `filter` and `map` stages, accepts configured `ProcessorFactory` stages through `via`, and terminates in one or more `ExporterFactory` stages with `to`.

```java
var traces = Pipeline.builder(SignalDescriptor.TRACES)
        .filter(batch -> batch.spanCount() > 0)
        .map(MyTransforms::redact)
        .to(OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(4317))
        .build();

try (traces) {
    traces.start();
    DeliveryResult result = traces.submit(batch).toCompletableFuture().join();
    FlushReport report = traces.flush(Duration.ofSeconds(5)).toCompletableFuture().join();
}
```

Without buffering, `submit` completes with the direct downstream delivery outcome. Acceptance by an explicit buffer transfers ownership; use `flush` to observe subsequent partial rejections, failures, and drops. Direct delivery outcomes are not reported again by flush. Flush captures each application input's in-progress deliveries first; downstream buffers snapshot when their component is reached, so concurrent submissions may be included. Exact global exclusion requires barrier propagation through every buffering component. `shutdown` is explicit; `close` applies the runtime's configured shutdown deadline.

## Domain model: `otlp4j-model`

`dev.nthings.otlp4j.model` contains immutable, proto-free records:

- `TracesData`, `MetricsData`, `LogsData`, and `ProfilesData` are signal batches.
- `Resource`, `InstrumentationScope`, and `Attributes` carry common metadata.
- `Span`, `Metric`, `LogRecord`, and the point/exemplar records carry signal items.
- `AttributeValue` represents OTLP scalar, bytes, array, and map values.

The module has no transport or service dependency and can be used to construct batches independently.

## Contracts: `otlp4j-core`

`dev.nthings.otlp4j.core` is the protocol-neutral contract layer and re-exports the model module.

| Package | Main types | Role |
| --- | --- | --- |
| `core.signal` | `SignalDescriptor`, `SignalPair`, `Payload`, `RoutingMetadata` | Typed signals, immutable payloads, item counts, and routing metadata |
| `core.consumer` | `SignalConsumer`, `SignalConsumers`, `TelemetryInput`, `ConsumeContext`, `ClientMetadata`, `Deadline` | In-process delivery and request context |
| `core.component` | `Component`, `Receiver`/`Processor`/`Exporter`/`Connector`, their factories, `ComponentId`, `ComponentConfig` | Component lifecycle and extension contracts |
| `core.result` | `DeliveryResult`, `Disposition`, `DeliveryReason`, `Provenance`, `FlushReport` | Accepted, partial, retryable, and permanent outcomes |
| `core.policy` | `BatchPolicy`, `DeliveryQueuePolicy`, `RetryPolicy`, `BackoffPolicy`, `TimeoutPolicy` | Shared bounded-delivery and retry policy values |
| `core.pipeline` | `PipelineId` | Named signal-pipeline identity |
| `core.telemetry` | `GatewayMetrics` | Optional self-observability SPI; no-op by default |

`ProcessorFactory.map(type, signal, mapper)` and `ProcessorFactory.filter(type, signal, predicate)` cover the common synchronous transforms. `ExporterFactory.of(type, terminal)` creates a terminal exporter from a callback. Components that need richer behavior implement the corresponding factory contract directly.

## Graph runtime: `otlp4j-service`

`dev.nthings.otlp4j.service` provides the advanced graph API:

```java
var graph = Gateway.builder();
var receiver = graph.receiver(
        ComponentId.parse("in"),
        OtlpReceiverFactory.grpc(),
        OtlpReceiverConfig.defaults(4317).withEphemeralPort());
var exporter = graph.exporter(
        ComponentId.parse("out"),
        OtlpExporterFactory.grpc(),
        OtlpExporterConfig.defaults(4317));
graph.pipeline(PipelineId.of(TRACES), path -> path.from(receiver).to(exporter));

try (var runtime = graph.build()) {
    runtime.start();
    int port = runtime.boundPort(receiver.id()).orElseThrow();
}
```

The builder returns typed `ReceiverHandle`, `ProcessorHandle`, `ExporterHandle`, and `ConnectorHandle` values. `PipelineBuilder.from`, `via`, and `to` accept only handles of the right kind. `Gateway.compile(GatewayConfig)` is the direct configuration path for loaders and tools.

`GatewayRuntime` owns component startup, flush, shutdown, and graph inspection. Start order brings downstream components up before their inputs; shutdown stops inputs first and drains toward exporters. Start failures roll back every component created for the graph, including a component whose `start` call failed or was not reached. `boundPort` reads an ephemeral network receiver after startup.

`ServiceOptions` bounds lifecycle waiting, but Java cannot forcibly stop a component operation that ignores cancellation or interruption. Such an operation can finish after the caller's deadline; the runtime retains its late result and continues cleanup when the operation yields.

Application inputs in a gateway are represented by typed `InputHandle<T>` values. Resolve one from the built runtime with `runtime.input(handle)`; the resulting `TelemetryInput<T>` is valid only while that runtime is running and remains owned by the runtime.

## Wire transport: `otlp4j-transport`

`dev.nthings.otlp4j.transport` contains the Armeria implementation for OTLP/gRPC and OTLP/HTTP:

- `OtlpReceiverFactory.grpc()` and `.http()` create graph receivers.
- `OtlpExporterFactory.grpc()` and `.http()` create graph exporters.
- `OtlpReceiverConfig` and `OtlpExporterConfig` configure endpoints, TLS, headers, compression, retry, timeouts, and admission limits.
- `OtlpExporter.grpcBuilder()` and `.httpBuilder()` provide an owned standalone exporter for callers that do not need a graph.
- `OtlpReceiver.grpcBuilder()`, `.httpBuilder()`, and the dual-protocol `.builder()` provide owned standalone receivers with typed routes.
- `Tls`, `ClientAuth`, `Compression`, `Headers`, and `OtlpEnv` are transport configuration values.

The transport accepts binary protobuf over the standard OTLP paths. Protocol, Netty, and generated types remain behind the module's internal boundary.

Owned transports are unstarted until `start()`. Their `shutdown(timeout)` stages are idempotent and preserve failures for subsequent `close()` calls. Receiver shutdown is graceful: expiry of the caller's budget is reported while Armeria finishes cleanup within its configured drain window. There is no force-stop API.

The default limits are part of the configuration records and can be raised or lowered explicitly:

| Surface                  | Default                               |
| ------------------------ | ------------------------------------- |
| Receiver decoded request | 64 MiB maximum per export request     |
| Exporter request         | 64 MiB maximum per serialized request |
| Exporter response        | 4 MiB maximum per response            |

## Built-ins: `otlp4j-components`

The built-in module exports independent factory/config pairs for batching, memory limiting, filtering, routing, and trace-to-metric span counting:

| Package | Types |
| --- | --- |
| `components.batch` | `BatchProcessorFactory`, `BatchProcessorConfig` |
| `components.memory` | `MemoryLimiterProcessorFactory`, `MemoryLimiterConfig` |
| `components.routing` | `FilterProcessorFactory`, `FilterConfig`, `RoutingProcessorFactory`, `RoutingConfig`, `MatchRule`, `MatchSpec` |
| `components.connector` | `SpanCountConnectorFactory`, `SpanCountConnectorConfig` |

`BatchProcessorConfig.defaults()` targets 8192 items or one second, with no hard batch item/byte cap. Its delivery queue holds 1024 entries, uses one consumer, and applies `OverflowPolicy.FAIL`. The processor also bounds accepted work at 1024 submissions, 64 MiB of exact serialized bytes, and 1024 metadata partitions. The processor rejects profiles at graph validation time; it cannot merge distinct opaque profile dictionaries without re-indexing.

## Optional SDK bridge: `otlp4j-opentelemetry`

The bridge module is the dependency boundary for adapters to the official OpenTelemetry Java SDK. It depends on the SDK trace, metrics, and logs exporter contracts while the model, core, service, and transport modules remain independent of the SDK. Add it only when an application needs to connect an SDK exporter to an in-process otlp4j input.

The adapters borrow their `TelemetryInput`; they snapshot SDK data into the immutable model, and their `shutdown()` drains accepted input work without closing or stopping the input. Close the SDK provider before its borrowed pipeline. SDK exporter shutdown waits for outstanding input submissions and then performs the adapter's bounded input flush, so an input implementation must complete its submission stages:

```java
var traces = Pipeline.builder(SignalDescriptor.TRACES)
        .to(OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(4317))
        .build();
var sdkExporter = SdkExporters.spans(traces);
var tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(sdkExporter))
        .build();

try (traces; tracerProvider) {
    traces.start();
    // Instrumentation uses tracerProvider; completed spans enter the pipeline.
}
```

Use `SdkExporters.metrics(metricsInput)` for the default cumulative aggregation temporality, or pass an explicit official `AggregationTemporalitySelector` when the SDK reader requires a different policy. `SdkExporters.logs(logsInput)` provides the equivalent log-record adapter.

The bridge targets OpenTelemetry Java SDK 1.65.0 and maps all fields exposed by its exporter interfaces. SDK exporter data has no OTLP point flags, exponential-histogram zero threshold, resource or instrumentation-scope dropped-attribute counts, or metric metadata; those model fields therefore use their defaults (zero flags/counts/threshold and empty metadata). Span and link remote context bits are preserved, and SDK collections and byte values are snapshotted before submission.

## Internal modules

`otlp4j-codec` maps domain payloads to generated protobuf messages. `otlp4j-proto` contains generated messages and gRPC stubs. Their packages are qualified-exported only to the bundled implementation modules and should not be used as application dependencies.
