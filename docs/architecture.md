# Architecture

otlp4j is an in-process OTLP data plane. It receives or accepts typed batches, runs them through a small graph of components, and exports the resulting batches. The design keeps the protocol boundary at the edge while making ordinary application code work with immutable Java records.

## Layering

```text
application
    |
    +--> otlp4j-service -- typed Pipeline / Gateway graph
    |          |
    |          +--> otlp4j-components -- reusable processors and connectors
    |          |
    |          +--> otlp4j-transport -- Armeria OTLP gRPC and HTTP
    |
    +--> otlp4j-model -- immutable traces, metrics, logs, profiles
               ^
               |
otlp4j-core ---+--- protocol-neutral contracts and payloads
    ^
    |
otlp4j-codec / otlp4j-proto -- internal protobuf marshalling and generated code
```

The model and core modules have no wire dependency. `otlp4j-codec` and `otlp4j-proto` are qualified JPMS boundaries; generated classes are available only to the bundled codec and transport implementations. `otlp4j-opentelemetry` is an optional separate boundary for official OpenTelemetry Java SDK adapters.

## Dependency choices

Java 25 virtual threads keep lifecycle waits, queue workers, and blocking transport boundaries simple without tying up platform threads. Armeria supplies the pooled client/server machinery and protocol handling; otlp4j layers its retry and server-hint policy on top while enforcing request-size, decompression, and in-flight admission limits at the wire boundary. The official OpenTelemetry Java SDK remains responsible for application instrumentation, context propagation, sampling, and metric aggregation; the optional bridge only snapshots SDK exporter data into otlp4j batches. JGraphT stays an internal implementation detail for validated DAG construction and topological ordering, so replacing it is unnecessary unless its dependency surface becomes a measured problem.

## Two composition levels

### A single typed pipeline

`Pipeline<T>` is the normal entry point. `Pipeline.builder(signal)` creates an application input for one signal, then `filter`, `map`, `via`, and `to` append stages. Its generic parameter means a `Pipeline<TracesData>` cannot accept `MetricsData`.

```text
submit(TracesData)
      |
      v
  application input -> filter/map -> configured processors -> OTLP exporter
      |                                                        |
      +---------------- flush report -------------------------+
```

The pipeline owns the graph runtime and transport. `submit` completes when a direct consumer accepts or refuses a batch, or when an explicitly configured buffer accepts it; buffering is explicit in the graph configuration. It does not wait for every downstream export. `flush(timeout)` first captures and drains deliveries already in progress at each application input, then snapshots downstream buffers as each component is reached and reports partial rejections, drops, and whole-batch failures. A submission concurrent with a downstream snapshot may be included; exact global exclusion requires propagating a barrier through every buffering component. `close()` performs bounded shutdown.

### A named component graph

`Gateway.builder()` is for network receivers, several signal pipelines, fan-out, connectors, or components shared across paths. Registration returns typed handles:

```text
ReceiverHandle --> ProcessorHandle --> ExporterHandle
       |                 |
       +-----------------+--> ConnectorHandle --> another pipeline
```

`PipelineBuilder.from(...).via(...).to(...)` accepts only handles of the corresponding kind. The builder compiles to `GatewayConfig`; `Gateway.compile(config)` supports callers that construct the configuration records directly.

The graph compiler validates component references, signal support, pipeline endpoints, and connector signal pairs before creating components. It reports all validation diagnostics in one `GraphValidationException`. A gateway application input is declared as an `InputHandle<T>` and resolved after build with `GatewayRuntime.input(handle)`; the runtime owns that input's lifecycle.

## Component contracts

Every component is lifecycle-bearing and receives a `ComponentContext`. The four component kinds are:

| Kind | Contract | Responsibility |
| --- | --- | --- |
| Receiver | `ReceiverFactory` | Accepts wire or application input and forwards one signal |
| Processor | `ProcessorFactory` | Filters, transforms, buffers, or otherwise forwards one signal |
| Exporter | `ExporterFactory` | Terminates a pipeline and reports delivery outcomes |
| Connector | `ConnectorFactory` | Bridges one signal pipeline to another using a `SignalPair` |

Factories expose a type, a default configuration, and a typed `create` method. A factory can implement `SignalSupport` to declare a subset of the four signals; otherwise the graph treats the generic factory as signal-capable. `ComponentConfig.NONE` represents a component with no configuration.

The inline core factories cover common cases:

- `ProcessorFactory.map` applies a synchronous, typed batch transform.
- `ProcessorFactory.filter` drops a batch as an accepted local outcome when its predicate is false.
- `ExporterFactory.of` turns a terminal callback into an exporter.

## Payload and model boundaries

`Payload<T>` is the unit passed between consumers. It always carries a `SignalDescriptor<T>`, an immutable domain batch, an item count, and resource-level `RoutingMetadata`. A domain-backed payload is created with `Payload.of(signal, batch)`.

Transport internals may decode the domain batch lazily, but callers only see the typed `data()` method. This keeps processing code independent of generated protobuf while allowing a wire implementation to avoid unnecessary work on paths that do not inspect the domain. Replacing data with `withData` creates a new payload and recomputes its derived metadata.

The model mirrors the OTLP grouping hierarchy (`resource -> instrumentation scope -> item`) for each signal. `SignalDescriptor` owns item counting, merge, split, and empty-batch operations so processors do not need a signal-specific switch.

Profiles remain experimental. Their opaque dictionary-bearing representation can be forwarded without re-indexing; batching distinct non-empty profile dictionaries is rejected because re-indexing would be required to preserve references.

The built-in `BatchProcessorFactory` therefore declares traces, metrics, and logs as supported signals and rejects profiles during graph validation. A profile path must forward each payload without batch merging, or supply a component that preserves the profile dictionary contract.

## Delivery semantics

Consumers return `CompletionStage<DeliveryResult>`. The result carries provenance and is one of:

| Result | Meaning |
| --- | --- |
| `Accepted` | Every item was accepted |
| `Partial` | Downstream accepted the batch but reported rejected items |
| `Rejected` with `RETRYABLE` | The whole batch was refused and may be retried |
| `Rejected` with `PERMANENT` | The whole batch was refused and should not be retried |

`ConsumeContext` carries client metadata, a deadline, and cancellation. A receiver can preserve request-scoped information for routing or policy decisions. `FlushReport` aggregates partial rejections, queue drops, failed batches, and the first available failure without retaining an unbounded error list.

## Lifecycle and ordering

The graph is a DAG. Start walks reverse topological order so downstream consumers are ready before their inputs. If startup fails, every component created for the graph is rolled back, including the component whose start failed and components not reached by the start walk. Shutdown walks forward topological order so receivers stop first and in-flight work can drain toward exporters. Each component operation is bounded by the configured `ServiceOptions` deadline, and remaining shutdown work continues after one failure. A component that ignores cancellation or interruption cannot be forcibly stopped by Java; it may finish after the caller's deadline, with the runtime retaining its late result and continuing cleanup when it yields.

## Transport

`otlp4j-transport` provides one Armeria implementation with protocol-parameterized factories:

- `OtlpReceiverFactory.grpc()` (port 4317) and `.http()` (port 4318).
- `OtlpExporterFactory.grpc()` (port 4317) and `.http()` (port 4318).

Receiver configuration includes bind address, TLS/client-auth settings, decoded-message size, and optional in-flight request/byte caps. Exporter configuration includes endpoint/path, headers, TLS, compression, retry, and per-attempt/total timeouts. HTTP carries binary protobuf on standard OTLP paths; JSON is outside this project.

The default hardening limits are 64 MiB for a receiver's decoded request, 64 MiB for an exporter's serialized request, and 4 MiB for an exporter's response. The built-in batch processor defaults to a 1024-entry, one-consumer queue with `OverflowPolicy.FAIL`; its accepted-work caps are 1024 submissions, 64 MiB of exact serialized bytes, and 1024 metadata partitions. Its trigger is 8192 items or one second, with no hard item/byte batch cap. Profiles are not supported by that processor because dictionary-preserving merge is not generally lossless.

## Observability and extension points

`GatewayMetrics` is an optional no-op-by-default SPI for export outcomes, admission rejections, queue drops, and connector activity. Components can receive it through `ComponentContext`. New components implement a core factory contract and can be packaged independently of the transport; a component's public configuration should use immutable records and core policy values.
