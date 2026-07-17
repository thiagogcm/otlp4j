# Public API

This is a map of every exported package to its purpose and the types you build against. Application code depends on `otlp4j-core` (the contracts, which re-exports `otlp4j-model` transitively), `otlp4j-transport` (the OTLP/gRPC and OTLP/HTTP transport), and `otlp4j-service` (the graph runtime); add `otlp4j-components` for the built-in components. The generated proto module is not user-facing (its packages are qualified-exported only to the codec and transport), and `otlp4j-codec` is an internal marshalling module exported only to the bundled transports and components.

Every exported type is `@NullMarked`: a reference is non-null unless annotated `@Nullable`.

## otlp4j-model

**`dev.nthings.otlp4j.model`** is the proto-free OTLP domain. The four signal batches are `TracesData`, `MetricsData`, `LogsData`, and `ProfilesData`, each with nested resource/scope records and fluent builders (`Span.builder()`, `Metric`, `LogRecord`, `NumberPoint`, and so on). The attribute model is `Attributes`, `AttributeValue`, `Resource`, and `InstrumentationScope`. These are the records a processor or exporter reads once a payload is materialized to the domain.

## otlp4j-core

The contracts every other module builds on. Proto-free; re-exports the model transitively.

| Package | Purpose | Key types |
| --- | --- | --- |
| `core.component` | The component SPI and its four factory kinds, plus the lifecycle and context objects. | `Component`, `ComponentId`, `ComponentKind`, `ComponentConfig` (and `ComponentConfig.NONE`), `ComponentContext`, `Receiver`/`ReceiverFactory`, `Processor`/`ProcessorFactory`, `Exporter`/`ExporterFactory`, `Connector`/`ConnectorFactory`, `CreateContext`, `FlushContext`, `ShutdownContext`, `NetworkBound`, `SharedComponents`, `SignalSupport`, `PayloadFactory` |
| `core.consumer` | The consume SPI the graph wires components through. | `SignalConsumer`, `SignalConsumers`, `ConsumeContext`, `ConsumerCapabilities`, `ClientMetadata`, `Deadline` |
| `core.signal` | Signal identity, the opaque payload, and its views. | `SignalDescriptor` (`TRACES`/`METRICS`/`LOGS`/`PROFILES`), `SignalPair`, `TelemetryPayload`, `PayloadView` (`DOMAIN_IMMUTABLE` and the metadata views), `RoutingMetadata` |
| `core.result` | The delivery outcome vocabulary. | `DeliveryResult` (`Accepted`/`Partial`/`Rejected`), `Disposition`, `DeliveryReason`, `Provenance` |
| `core.policy` | The neutral policy records components and the transport configure with. | `BatchPolicy`, `DeliveryQueuePolicy`, `OverflowPolicy`, `RetryPolicy`, `BackoffPolicy`, `TimeoutPolicy` |
| `core.pipeline` | Pipeline identity. | `PipelineId` (`of(signal)` / `of(signal, name)`) |
| `core.telemetry` | The self-observability SPI. | `GatewayMetrics` (with `noop()`), `GatewayMetrics.LongCounter`, `GatewayMetrics.Registration` |

The inline factory seams live here as statics: `ProcessorFactory.of(type, signal, transform, payloads)` and `ExporterFactory.of(type, terminal)`.

## otlp4j-codec

**`dev.nthings.otlp4j.codec`** is the model-to-proto marshalling layer, an internal module usable only by the bundled transports and components. The one type application-level code names is the `Payloads` facade: `Payloads.domain(signal, batch)` constructs a domain-backed payload, and `Payloads.DOMAIN` is the `PayloadFactory` emit seam an inline `ProcessorFactory.of` transform writes through. `SignalCodec` is the per-signal marshalling entry the transport uses; the mappers behind it are package-private.

## otlp4j-service

The graph runtime.

| Package | Purpose | Key types |
| --- | --- | --- |
| `service` | The builder, the runtime, and the typed pipeline-wiring handles. | `Gateway` (`builder()` / `compile(config)`), `GatewayBuilder`, `GatewayRuntime` (`start`, `shutdown`, `forceFlush`, `close`, `boundPort`), `PipelineBuilder`, `ReceiverHandle`/`ProcessorHandle`/`ExporterHandle`/`ConnectorHandle`, `PipelineSource`/`PipelineTarget`, `ComponentRef`, `GatewayStartException` |
| `service.config` | The declarative artifact a programmatic or future YAML caller constructs directly. | `GatewayConfig`, `PipelineConfig`, `ServiceOptions`, and the typed `ReceiverEntry`/`ProcessorEntry`/`ExporterEntry`/`ConnectorEntry` |
| `service.validation` | The validation diagnostics. | `Diagnostic`, `DiagnosticCode`, `GraphValidationException` |

## otlp4j-transport

**`dev.nthings.otlp4j.transport`** is the consolidated OTLP transport on Armeria. The graph factories are `OtlpReceiverFactory` and `OtlpExporterFactory`, each with `grpc()` and `http()` statics; their configs are `OtlpReceiverConfig` and `OtlpExporterConfig` (with `defaults(port)`, `builder(port)`, and `withEphemeralPort()`). The standalone entry points are `OtlpReceiver` and `OtlpExporter` (`grpcBuilder()` / `httpBuilder()`), building `ServerReceiver` and `ClientExporter`. The shared config value types are `Tls`, `Headers`, `Compression`, and `OtlpEnv` (the `OTEL_EXPORTER_OTLP_*` environment binding). Armeria, Netty, and proto types stay in the unexported `internal` package.

## otlp4j-components

The built-in components, each a factory plus its config record. Depends only on the core contracts and the codec facade, so a component here assembles into any graph without dragging in a server or client runtime.

| Package | Component | Types |
| --- | --- | --- |
| `components.batch` | Batching processor. | `BatchProcessorFactory`, `BatchProcessorConfig` |
| `components.memory` | Memory-limiter processor. | `MemoryLimiterProcessorFactory`, `MemoryLimiterConfig` |
| `components.routing` | Filter and routing processors over a match engine. | `FilterProcessorFactory`/`FilterConfig`, `RoutingProcessorFactory`/`RoutingConfig` |
| `components.connector` | Trace-to-metric span-count connector. | `SpanCountConnectorFactory`, `SpanCountConnectorConfig` |
