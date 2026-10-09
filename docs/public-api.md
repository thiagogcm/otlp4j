# Public API map

Use this page to locate public types and extension contracts. Task-oriented examples live in the linked guides; method-level reference is generated from source during [verification](development.md#build-and-check).

Public library modules use explicit JPMS exports. Reference types are non-null by default in packages marked `@NullMarked`; exceptions use explicit nullable annotations. Generated protocol classes are covered by the [internal module boundary](architecture.md#jpms-encapsulation).

## Entry points

| Need | Main public types | Guide |
| --- | --- | --- |
| One in-process signal flow | `Pipeline<T>`, `Pipeline.Builder<T>` | [Pipelines](pipelines.md) |
| Several component paths | `Gateway`, `GatewayBuilder`, `PipelineBuilder`, `GatewayRuntime` | [Gateways](gateways.md) |
| Typed component references | `InputHandle<T>`, `ReceiverHandle`, `ProcessorHandle`, `ExporterHandle`, `ConnectorHandle` | [Gateway wiring](gateways.md#wire-named-components) |
| Programmatic graph configuration | `GatewayConfig`, `PipelineConfig`, component entry records, `ServiceOptions` | [Validation and inspection](gateways.md#validate-and-inspect-a-graph) |
| Graph diagnostics | `GraphValidationException`, `Diagnostic`, `DiagnosticCode` in `service.validation` | [Gateway validation](gateways.md#validate-and-inspect-a-graph) |
| Standalone wire ownership | `OtlpReceiver`, `OtlpExporter` | [Owned transports](transport.md#owned-transports) |
| Graph wire components | `OtlpReceiverFactory`, `OtlpExporterFactory` | [Transport](transport.md) |
| SDK exporter adaptation | `SdkExporters` | [SDK integration](sdk-integration.md) |
| Telemetry values | Types in `dev.nthings.otlp4j.model` | [Domain model](model.md) |

Service builders, handles, and runtimes are in `dev.nthings.otlp4j.service`; configuration records are in `.service.config`. Transport types are in `.transport`; SDK adapters are in `.opentelemetry`.

## Core contracts

All packages below have the prefix `dev.nthings.otlp4j.core`.

| Package | Main types | Responsibility |
| --- | --- | --- |
| `.signal` | `SignalDescriptor<T>`, `SignalPair<I, O>`, `Payload<T>`, `RoutingMetadata`, `PayloadSizer` | Signal identity, batch views, and payload weight |
| `.consumer` | `SignalConsumer<T>`, `SignalConsumers`, `TelemetryInput<T>` | Delivery callbacks and application input |
| `.consumer` | `ConsumeContext`, `ClientMetadata`, `Deadline` | Request metadata, time budget, cancellation |
| `.component` | `Component`, `Receiver`, `Processor<T>`, `Exporter<T>`, `Connector` | Lifecycle and processing components |
| `.component` | Component factories, `ComponentConfig`, `ComponentId`, `ComponentKind` | Construction, configuration, identity |
| `.component` | `ComponentContext`, `CreateContext`, `FlushContext`, `ShutdownContext`, `SignalSupport`, `NetworkBound` | Runtime context and capabilities |
| `.result` | `DeliveryResult`, `Disposition`, `DeliveryReason`, `Provenance`, `FlushReport` | Delivery and drain outcomes |
| `.policy` | `BatchPolicy`, `DeliveryQueuePolicy`, `OverflowPolicy`, `RetryPolicy`, `BackoffPolicy`, `TimeoutPolicy` | Immutable policy values |
| `.pipeline` | `PipelineId` | Signal and optional pipeline name |
| `.telemetry` | `GatewayMetrics` | Counters and gauges for component health |

`SignalDescriptor` has four fixed descriptors with generic batch types. A `SignalPair` identifies a connector's input and output signals. Policies are configuration values; the component or transport implements their behavior.

## Implement a component

Choose the factory contract for the component's position:

| Factory               | Constructed role                               |
| --------------------- | ---------------------------------------------- |
| `ReceiverFactory<C>`  | Source with a typed downstream consumer        |
| `ProcessorFactory<C>` | Stage consuming and forwarding one signal      |
| `ExporterFactory<C>`  | Terminal consumer for one signal               |
| `ConnectorFactory<C>` | Bridge for a declared input/output signal pair |

A factory supplies its type, kind, default configuration, and creation method. Use an immutable record implementing `ComponentConfig`, or `ComponentConfig.NONE` when no configuration is needed. A factory can implement `SignalSupport` to restrict accepted signals; otherwise generic receiver, processor, and exporter factories are treated as signal-capable. Connectors declare `supportedPairs()`.

The core includes helpers for small components:

- `ProcessorFactory.map(type, signal, transform)` wraps a synchronous domain transform.
- `ProcessorFactory.filter(type, signal, predicate)` wraps a whole-batch predicate.
- `ExporterFactory.of(type, terminal)` wraps an asynchronous terminal callback.

The [first pipeline](getting-started.md#run-a-first-pipeline) demonstrates a terminal callback. Use a full component implementation when you need owned resources, scheduling, buffering, or custom lifecycle.

Implementations must return non-null completion stages and outcomes, release resources during shutdown, and report delayed losses when flushing buffered work. Preserve request context until ownership transfers. Use the rules in [Delivery and lifecycle](delivery.md) rather than inventing another acknowledgement contract.

## Self-observability

`GatewayMetrics` defines a monotonic counter handle and a sampled gauge registration. Implementations must be thread-safe because components report from concurrent processing and delivery threads. Close gauge registrations when their owners stop.

Install an implementation through `GatewayBuilder.build(new ComponentContext(metrics, clock))` or an owned transport builder's `.telemetry(metrics)`. Unconfigured runtimes use `GatewayMetrics.noop()`. The [`LoggingGatewayMetrics` sample](../otlp4j-samples/src/main/java/dev/nthings/otlp4j/samples/LoggingGatewayMetrics.java) illustrates an adapter.

Bundled transport instruments include exporter outcomes and receiver admission counts and gauges. The SPI is not an automatic bridge to a monitoring backend, and the SDK exporter bridge does not implement such a backend.

## Component and transport configuration

Use [Built-in components](components.md) for factory/configuration pairs and their supported signals. Use [Transport](transport.md) for `OtlpReceiverConfig`, `OtlpExporterConfig`, `Tls`, `ClientAuth`, `Compression`, and header configuration.

`Headers` validates outgoing header names and values. Environment parsing is reached through the exporter builder's `fromEnvironment()` method; its parser is an internal implementation detail.
