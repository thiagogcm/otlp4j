# Architectural Evaluation: Reshaping otlp4j into an Extensible OpenTelemetry Gateway

## Evaluation Boundary

This report evaluates the current source tree, not past plans or history. The OpenTelemetry comparison uses the local upstream checkouts requested for this evaluation:

- `$HOME/gitbase-data/open-telemetry/opentelemetry-java`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector`

Third-party library recommendations are based on the current repository dependencies and primary library documentation for Armeria, Helidon, Resilience4j, Failsafe, JCTools, JGraphT, and protobuf-java. This report decides them: JGraphT internal to `otlp4j-service`, Armeria as the transport runtime implementing retry and timeout with its built-in decorators (the Phase 4 opening spike is the only reopening condition), Resilience4j retained only inside the legacy transports until Phase 4 deletes them, Failsafe and JCTools rejected. The strict local-reference constraint applies to the OpenTelemetry SDK and Collector architecture comparison.

## Executive Verdict

otlp4j is currently a compact JVM-native OTLP pipeline library with a fluent in-process wiring API. It is not yet a Collector-like gateway runtime. The dominant architectural gap is that the public API models "source to sink chains" rather than a component graph made of receivers, processors, exporters, connectors, and extensions. This makes the current system easy to embed but difficult to extend as a gateway.

The strongest blueprint is a breaking redesign around these principles:

1. Introduce a Collector-style service graph as the primary runtime, modeled internally on JGraphT, with every graph-shaped change in this report landing together as the initial refactoring step.
2. Decouple telemetry signal metadata and payload representation from the immutable model.
3. Move batching, queues, admission, and lifecycle out of fluent pipeline wiring and into reusable component runtime helpers; retry and timeout move to the transport runtime's built-in support.
4. Treat the current immutable model as an optional domain view, not the mandatory hot-path representation.
5. Consolidate OTLP/HTTP and OTLP/gRPC on Armeria as the single production transport runtime, implemented in Phase 4 behind the transport SPI and gated by an opening validation spike.
6. Pin the OTLP wire contract with a black-box conformance suite and a performance baseline before the redesign begins; it is the only interface that survives the break.

The intended end state is a JVM gateway that can be configured and extended like the OpenTelemetry Collector while preserving otlp4j's Java-first ergonomics for embedded use.

## Current Architecture Snapshot

### Modules

The repository currently publishes these functional modules:

| Module | Current role | Architectural assessment |
| --- | --- | --- |
| `otlp4j-model` | Immutable domain records, `Signal`, `SignalBatch`, `ConsumeResult`. | Valuable domain facade, but too central to the runtime hot path. |
| `otlp4j-proto` | Generated OpenTelemetry proto classes. | Correctly isolated; should remain the wire-schema module. |
| `otlp4j-api` | Pipeline DSL, source/sink/processor APIs, receiver/exporter SPI, config, telemetry. | Too broad. It mixes public user API, runtime internals, config, lifecycle, and resilience-library exposure. |
| `otlp4j-codec` | Proto-to-model and model-to-proto mappers. | Functional, but forces full materialization and reverse translation even for simple forwarding. |
| `otlp4j-transport-common` | Receiver/exporter base classes, admission, retry helpers, signal source. | Right place for shared transport code, but it currently blends transport, exporter facade, admission, and lifecycle. |
| `otlp4j-transport-http` | JDK HTTP receiver/exporter. | Lightweight, but not robust enough for a gateway runtime. |
| `otlp4j-transport-grpc` | grpc-java/Netty receiver/exporter. | Stronger protocol base than HTTP, but duplicates client/exporter behavior and uses blocking stubs on virtual threads. |
| `otlp4j-testing` | Test helpers. | Should become the conformance bed for component graph, pipeline order, errors, and backpressure. |
| `otlp4j-samples` | Examples. | Should shift from fluent embedding examples to service-graph examples. |

### Current Data Flow

The effective flow is:

```text
HTTP or gRPC request
  -> generated protobuf request
  -> otlp4j-codec mapper
  -> immutable otlp4j-model batch
  -> ServerReceiver / SignalSource
  -> Pipeline.from(source).transform/filter/via/to(...)
  -> Sink / Batcher / Connector / FanOut
  -> ClientExporter signal facet
  -> OtlpClient implementation
  -> otlp4j-codec mapper
  -> generated protobuf request
  -> HTTP or gRPC client send
```

This is clean for a demo or embedded library. It is inefficient and constraining for a gateway:

- Every received OTLP request is decoded into the domain model before the runtime knows whether a processor needs a domain view.
- Every export rematerializes protobuf messages from the domain model.
- `Signal` owns merge and item-count logic, so batching behavior is coupled to the model module.
- The runtime has no first-class graph, component registry, extension surface, or declarative pipeline identity.
- Lifecycle is discovered from object shape and `instanceof` checks rather than declared by graph nodes.

## Upstream Alignment and Divergence

### OpenTelemetry Java SDK

Relevant SDK patterns in the local `opentelemetry-java` checkout:

- Exporters expose simple async lifecycle operations: `export`, `flush`, `shutdown`, and `close`.
- `CompletableResultCode` is intentionally small: success/failure, aggregation, and bounded join.
- `SpanExporter`, `MetricExporter`, and `LogRecordExporter` provide composite/noop helpers and clear lifecycle expectations.
- `MetricExporter` exposes a `MemoryMode` for reusable versus immutable metric data. This is useful inspiration for memory-aware contracts, not a direct precedent for proto/raw/domain gateway payload modes.
- SDK batch processors are specialized signal processors, not a general gateway graph.

Alignment:

- otlp4j already has async export results and lifecycle semantics.
- `ClientExporter` exposes signal-specific facets, which is close to SDK signal-specific exporters.

Divergence:

- The SDK is not a gateway architecture. It should influence lifecycle vocabulary and memory-mode awareness, not the pipeline model.
- otlp4j currently uses `ConsumeResult` for gateway protocol decisions, which is more expressive than `CompletableResultCode` and should remain separate from SDK result types.

Recommendation:

- Keep a gateway-specific result model.
- Use Collector lifecycle vocabulary for components: `start` and `shutdown`.
- Keep one flush verb across the runtime: `forceFlush`, which the existing `Lifecycle` contract already uses. Splitting the vocabulary into SDK-style `flush` for exporters and `forceFlush` for processors names the same operation twice and contradicts the uniform `Component` contract proposed below.
- Add a gateway-specific payload representation contract. It can be inspired by SDK `MemoryMode`, but it must define its own correctness rules for proto, domain, and raw payload handling.

### OpenTelemetry Collector

Relevant Collector patterns in the local `opentelemetry-collector` checkout:

- `component.Component` is the uniform lifecycle contract for receivers, processors, exporters, connectors, and extensions.
- Factories own component type, default config, signal support, and stability level.
- Receivers translate input formats and feed a downstream consumer.
- Processors are both components and consumers.
- Exporters are terminal consumers.
- Connectors act as exporters from one pipeline and receivers to one or more downstream pipelines.
- Pipelines are identified by signal and optional name.
- The service graph validates component availability, signal compatibility, connector usage, duplicate processors, cycles, and lifecycle order.
- Consumers declare capabilities such as mutation.
- `consumererror` separates permanent, retryable, and downstream errors.

Alignment:

- otlp4j has the right nouns: receivers, processors, sinks/exporters, connectors, fanout, and partial/retryable outcomes.
- `ServerReceiver` is similar to a multi-signal receiver instance.
- `ClientExporter` is similar to one configured exporter with signal-specific consumer facets.
- `FanOut` is safe because otlp4j model batches are immutable.

Divergence:

- otlp4j has no component factories or registry.
- `Pipeline` is a fluent chain builder, not a configured service graph.
- `Connector<I,O>` is a sink-side transform, not a graph component that is both exporter and receiver across pipelines.
- `Wiring` validates object wiring after the fact, not a declarative graph before construction.
- Error taxonomy is flattened into `ConsumeResult`, so "local component failed" and "downstream refused" are not strongly separated.
- `Signal` is closed and implemented in the model module, while Collector supports evolving signals through signal-specific factory methods and x-packages.

Recommendation:

- Use Collector as the target gateway shape: component factories, pipeline IDs, graph validation, connector routing, and start/shutdown ordering.
- Keep Java-specific type safety and asynchronous return types, but make the public architecture recognizable to Collector component authors.

## Architectural Findings

### P0: No Service Graph Runtime

`Pipeline.java` builds one source-to-terminal chain. `Wiring.java` bundles receivers, pipeline handles, and owned resources. This is not enough for a gateway because components are not addressed by type/name IDs, pipelines cannot be validated declaratively, connectors are not graph edges, and shared receivers/exporters are discovered indirectly.

Impact:

- Third-party developers cannot write Collector-like components.
- The runtime cannot validate connector topology before start.
- Start and shutdown order depend on hand-built wiring.
- There is no stable place for extensions such as auth, health, memory limiter, or storage.

Required change:

- Add `otlp4j-service` as the primary graph runtime.
- Model the service graph internally on JGraphT (`org.jgrapht:jgrapht-core`): `DirectedAcyclicGraph` rejects cycle-creating edges at insertion, `TopologicalOrderIterator` yields deterministic build/start/shutdown order, and its connectivity inspectors surface orphaned components with precise diagnostics. JGraphT stays an internal dependency of `otlp4j-service`; no JGraphT type appears in public API.
- Introduce component IDs, component kinds, factories, registry, service config, and graph builder.
- Land all graph-shaped work in this report as one initial refactoring step: this finding, connector topology (the `Connector` finding), declared lifecycle ordering (the lifecycle finding), and the `Wiring`/`PipelineLifecycle` replacement are one refactoring unit, not separable workstreams.
- Delete the fluent pipeline DSL from the public gateway surface; `otlp4j-embedding` keeps the small `EmbeddedPipeline` facade, which compiles to `GatewayConfig`.

### P0: Signal Batch Policy Is Welded to the Model Module

`Signal<T>` is a closed generic-enum-like class with four constants and private merge/item-count behavior. The closed value set is not the defect; the Collector treats the signal set as closed too. The defect is that merge, item count, and empty-batch construction are runtime batching policy implemented inside `otlp4j-model`, and the profiles merger already breaks the generic contract by throwing `IllegalStateException` on distinct dictionaries, which proves batch policy is per-signal runtime behavior rather than model behavior.

Collector itself does not prove that arbitrary third-party signals are required; its factory APIs and pipeline identity are still centered on known signals, with profiles handled through experimental packages. Supporting arbitrary application-defined signals would be an otlp4j product choice beyond Collector compatibility.

Impact:

- New or experimental OTLP signals require core edits.
- Batching, partial success, and item counting are not component-level policies.
- Profiles already have special merge behavior that does not fit generic batching.

Required change:

- Replace `Signal<T>` as the primary runtime key with `SignalDescriptor<T>` entries for built-in and experimental OTLP signals.
- Move merge, size, item count, response mapping, and codec behavior into descriptors or codecs.
- Arbitrary third-party signal registration is out of scope (a first-milestone non-goal); descriptors cover the four OTLP signals.

### P0: Codec-to-Model Translation Is a Hot-Path Bottleneck

`otlp4j-codec` converts generated proto requests into immutable model batches and converts them back to generated proto requests on export. This is simple, but it is the wrong default for a gateway whose common workload is receive, batch, route, and export with little or no data mutation.

Impact:

- Extra object allocation for every pass-through request.
- ID fields move through hex string representations and back.
- Full request bodies are buffered in HTTP before parse and are serialized with `toByteArray` before send.
- Processors that only need routing metadata pay for full model materialization.

Required change:

- Introduce a payload abstraction that can carry protobuf messages and materialize the domain model lazily.
- Add direct write APIs so transports can stream protobuf using `MessageLite.writeTo(OutputStream)` or transport-native buffers instead of always calling `toByteArray`.
- Make the immutable model an optional facade for user processors, not the internal transport representation.
- Raw-byte payload mode is deferred (a first-milestone non-goal). Its recorded design: a per-signal descriptor capability grounded in protobuf wire-format concatenation, where serialized OTLP export requests for traces, metrics, and logs merge correctly by byte concatenation because their only top-level field is repeated, while profiles are excluded because the singular dictionary field merges recursively and corrupts dictionary index references. Item counting, sizing, and partial-success mapping over raw bytes require a streaming scan, so a descriptor without them limits raw bytes to same-protocol, no-mutation, no-batch pass-through.

### P0: Public API Leaks Implementation Libraries

`otlp4j-api` declares `requires transitive io.github.resilience4j.retry` and `io.github.resilience4j.core` in its `module-info.java` because `ClientConfig` carries `RetryConfig` as a record component and `defaultRetryConfig()` returns it. The retry implementation is part of the user-facing module contract at the JPMS level, not merely a Maven scope accident.

Impact:

- Changing retry libraries becomes a breaking API decision.
- Users must understand a third-party retry model to configure a gateway exporter.
- Resilience policy is expressed as a third-party library type instead of neutral gateway configuration the transport can implement with its own machinery.

Required change:

- Replace public `RetryConfig` exposure first with neutral otlp4j records such as `RetryPolicy` and `BackoffPolicy`.
- The public policy surface is `RetryPolicy`, `BackoffPolicy`, `TimeoutPolicy`, `BatchPolicy`, `OverflowPolicy`, and `DeliveryQueuePolicy`; no circuit-breaker or bulkhead policy ships in the first milestone.
- Leave the Resilience4j bindings where they are, inside the legacy transports' internal helper; they are deleted with those modules in Phase 4, when Armeria's built-in retry and timeout support takes over.

### P0: Transport Runtime Is Split and Hand-Rolled

HTTP uses JDK `HttpServer` and `HttpClient`; gRPC uses grpc-java Netty. Retry policy execution is already shared through `ResilienceRetries` in `otlp4j-transport-common`, but each client still owns its retryable-error classification, pushback extraction, timeout, compression, header handling, shutdown, response mapping, and export execution.

Impact:

- HTTP has useful basics today: gzip request decoding, decoded body caps, admission refusal with retry-after, and `HttpServer.stop(...)` based graceful stop. It still lacks HTTP/2, middleware, explicit request/header/idle timeouts, pre-admission byte budgeting, and production server controls.
- gRPC and HTTP duplicate exporter behavior around the shared retry core: retryable classification, pushback mapping, timeout, and export metrics live twice.
- Blocking gRPC stubs and blocking HTTP sends on virtual threads are a sound Loom-first transport model for a Java 25 runtime; the defect is that the same resilience policy is hand-rolled twice, and one transport runtime implementing it once removes the duplication.

Required change:

- Consolidate both protocols on Armeria as the single production transport runtime (decided under Third-Party Library Integration); the current transports stay as is behind the new SPI until Phase 4 lands.
- Delegate retry, backoff, and timeout to Armeria's built-in retrying client and response timeouts, configured from the neutral policy records. The shared exporter helper owns only exporter metrics and `DeliveryResult` mapping; no queue or circuit-breaker helpers ship there, because the batch delivery queue owns both concerns.
- The transport exporter contract is `send(payload, context)`, honoring the configured retry and timeout policies internally. The hand-rolled retry code in both legacy clients is deleted with them in Phase 4, not rewritten.

### P1: Error Boundaries Are Too Flat

`ConsumeResult` distinguishes accepted, partial, and rejected outcomes, with retryable/permanent flags and retry-after. That is useful, but it does not encode whether a failure belongs to the current component, a downstream component, admission control, timeout, shutdown, or protocol translation.

Impact:

- Retry decisions can accidentally retry permanent local transformation failures.
- Fanout merges lose provenance.
- Async exceptions are normalized late and inconsistently across sources and transports.

Required change:

- Introduce `DeliveryResult` in `otlp4j-core` with component provenance, reason codes, and a downstream marker; `ConsumeResult` retires with the fluent API rather than being extended.
- Keep permanent, retryable, and downstream as the whole type-level taxonomy. Admission refusal, timeout, and shutdown are reason codes on `DeliveryResult`, never distinct wrapper types.
- Normalize every thrown exception at graph node boundaries.
- Preserve retry-after and rejected item counts in the result, not only in protocol adapters.

### P1: Backpressure and Admission Are Fragmented

`AdmissionControl` limits in-flight requests and decoded bytes at receivers. `Batcher` has its own bounded queue and overflow policy. Transports have independent request timeouts. There is no global memory limiter or consistent flow-control contract.

Impact:

- A gateway can accept decoded data that later accumulates in batch queues.
- HTTP admission byte accounting happens after full allowed decoded-body materialization.
- Processors and exporters have no common bounded execution policy.
- Retry, queueing, and admission are not coordinated.

Required change:

- Add an `AdmissionController` component with request, byte, queue, and memory budgets.
- Add a memory limiter processor or extension modeled after Collector's memory limiter.
- Introduce a component-level `ExecutionPolicy` for bounded concurrency.
- Propagate backpressure as retryable refusal with retry-after or gRPC pushback.

### P1: `Batcher` Has Too Many Responsibilities

`Batcher<T>` currently implements processor, sink, lifecycle, queue, scheduler, overflow policy, ack mode, a hand-rolled degraded-window circuit breaker, delivery timeout, and metrics. Retry is owned by the HTTP/gRPC clients through the shared `ResilienceRetries` helper, not by `Batcher`.

Impact:

- The same type tries to serve as a pipeline processor and a queued delivery boundary.
- Ack, timeout, overflow, circuit-breaker, and backpressure semantics are mixed with batch formation.
- Queue capacity counts batches, blind to both item count and bytes, so one queue slot can hold an arbitrarily large batch and the memory bound is nominal.
- An exporter-side sending queue is not needed: the delivery queue at the batch boundary plus transport retry already bounds delivery, and a second queue would only duplicate it.

Required change:

- Split batching into:
  - `BatchProcessor`: groups same-signal data before downstream processors/exporters.
  - A delivery queue helper: reusable internals for bounded delivery, optional wait-for-result behavior, and the existing degraded-window ingress refusal.
- Build no exporter `SendingQueue`: the delivery queue is the single queueing point, and a second queue returns only with persistent storage, which is a non-goal.
- Use neutral policies: `BatchPolicy`, `Sizer`, `OverflowPolicy`, `RetryPolicy`, `BackoffPolicy`, and `TimeoutPolicy`. Persistent storage is a non-goal.

### P1: `Connector` Is Underpowered

`Connector<I,O>` is a sink-side derivation from one input batch to zero or one output batch, then fanout to downstream sinks. Collector connectors are graph components used as exporters from one pipeline and receivers into one or more pipelines, with explicit signal-pair support.

Impact:

- Multi-output, async, routing, and cross-pipeline connectors are not first-class.
- Connector lifecycle and validation are hidden inside pipeline terminal classification.
- Existing `Connectors.spanCount` and similar helpers are useful examples but not a component model.

Required change:

- Replace `Connector<I,O>` with `ConnectorFactory` and `ConnectorComponent`.
- Model supported signal pairs explicitly, as edge metadata in the service graph, so pair validation and cycle rejection come from the graph model and land in the same initial refactoring step.
- Provide a `DerivingConnector` helper for simple zero/one derivations.
- Let connectors emit to named downstream pipelines through graph-provided routers.

### P1: Lifecycle Is Discovered Instead of Declared

`PipelineLifecycle` and `Wiring` infer lifecycle resources by inspecting terminal shapes such as `Batcher`, `Connector`, `FanOut`, `SharedLifecycle`, and `AutoCloseable`.

Impact:

- Hidden resources behind lambdas require `owns(...)`.
- Validation is advisory and after-the-fact.
- Graph-wide startup and shutdown cannot be proven.

Required change:

- Make lifecycle a property of graph components.
- Derive build, start, and shutdown order from the service graph's topological order instead of hand-maintained phase lists: build and start walk the graph in reverse topological order so exporters and processors precede receivers, and shutdown walks it forward so receivers stop first and queues drain into still-open exporters and extensions.
- Land this in the initial graph refactoring step; it is the same work as replacing `Wiring`.

## Target Architecture Blueprint

### Proposed Module Shape

| Proposed module | Purpose |
| --- | --- |
| `otlp4j-core` | Minimal public contracts: component ID, kind, signal descriptor, delivery result, lifecycle, context, capabilities, and the `TelemetryPayload` contract. |
| `otlp4j-model` | Immutable domain records, kept as the optional domain view that codecs materialize on demand. |
| `otlp4j-proto` | Generated OpenTelemetry protobuf classes. |
| `otlp4j-codec` | Signal codecs, payload materializers, response mappers, direct write/read helpers. |
| `otlp4j-service` | Component registry, service config, JGraphT-backed graph builder, validation, lifecycle orchestration, and the exporter metrics helper. |
| `otlp4j-components` | Built-in processors and helpers: batch, memory limiter, attributes/resource transforms, routing, filtering. |
| `otlp4j-transport-otlp` | Armeria-based OTLP HTTP/gRPC receiver and exporter factories, implementing the neutral retry and timeout policies with Armeria's built-in decorators (Phase 4). |
| `otlp4j-embedding` | Optional fluent API adapter for small embedded use cases. |
| `otlp4j-testing` | Component conformance fixtures, fake receivers/exporters, graph-order assertions. |

`otlp4j-api` is deleted at the end of Phase 1; no compatibility facade replaces it.

### Core Public Contracts

The public surface should be small and library-neutral:

```java
public interface Component extends AutoCloseable {
    CompletionStage<Void> start(ComponentContext context);
    CompletionStage<Void> forceFlush(FlushContext context);
    CompletionStage<Void> shutdown(ShutdownContext context);
}

public interface SignalConsumer<T extends TelemetryPayload> {
    ConsumerCapabilities capabilities();
    CompletionStage<DeliveryResult> consume(ConsumeContext context, T data);
}

public interface ReceiverFactory<C extends ComponentConfig> extends ComponentFactory<C> {
    Receiver create(CreateContext context, C config, SignalConsumers consumers);
}

/// The graph-routed consumers for every signal this receiver instance serves.
public interface SignalConsumers {
    Set<SignalDescriptor<?>> signals();
    <T extends TelemetryPayload> SignalConsumer<T> consumer(SignalDescriptor<T> signal);
}

public interface ProcessorFactory<C extends ComponentConfig> extends ComponentFactory<C> {
    <T extends TelemetryPayload> Processor<T> create(
            CreateContext context,
            C config,
            SignalDescriptor<T> signal,
            SignalConsumer<T> next);
}

public interface ExporterFactory<C extends ComponentConfig> extends ComponentFactory<C> {
    <T extends TelemetryPayload> Exporter<T> create(
            CreateContext context,
            C config,
            SignalDescriptor<T> signal);
}

public interface ConnectorFactory<C extends ComponentConfig> extends ComponentFactory<C> {
    Set<SignalPair<?, ?>> supportedPairs();
    Connector create(CreateContext context, C config, SignalPair<?, ?> pair, SignalConsumer<?> next);
}
```

Names can change during implementation. The important shape is:

- Components own lifecycle.
- Factories own config and signal support.
- Consumers are signal typed.
- Retry/batch/transport implementation types do not appear in public config.
- Capabilities are declared, not inferred.
- One component ID yields one component instance. A per-signal receiver `create` would instantiate the OTLP receiver once per signal, and four instances cannot share one port, so the receiver factory takes all routed signal consumers at once. Exporters and connectors referenced from multiple pipelines need the same guarantee from the graph builder; the Collector provides it through its `sharedcomponent` map keyed by config identity, and per-signal exporter facets over one shared client (the current `ClientExporter` shape) remain the correct Java expression of it.
- Factories declare a stability level (development, alpha, beta, stable), matching the Collector factory metadata this report already cites, so experimental components such as profiles support ship honestly without implying API stability.
- `ConsumeContext` carries receiver-captured client metadata: the authenticated principal and selected request headers or gRPC metadata. `BatchPolicy.metadataKeys` and metadata-based routing are unimplementable without it, and it cannot be retrofitted cheaply because it appears in every consumer signature; today's `Sink.consume(batch)` carries no context at all.

### Payload Representation

Add a transport-aware payload interface:

```java
public interface TelemetryPayload {
    SignalDescriptor<?> signal();
    long itemCount();
    long estimatedSerializedSize();
    PayloadMemoryMode memoryMode();
    <T> T view(PayloadView<T> view);
    TelemetryPayload copyForMutation();
    void writeTo(OutputStream out) throws IOException;
}
```

Built-in views:

- `PROTO_REQUEST`: generated OTLP request message.
- `DOMAIN_IMMUTABLE`: current `TracesData`, `MetricsData`, `LogsData`, `ProfilesData`.
- `RAW_BYTES`: original wire payload only for constrained pass-through cases.
- `ROUTING_METADATA`: cheap attributes useful for routing without full domain materialization.

This lets pass-through pipelines avoid unnecessary decode/re-encode while preserving the existing model for user processors that want normal Java records.

Ship order is decided: Phase 2 delivers `PROTO_REQUEST` and `DOMAIN_IMMUTABLE`; `ROUTING_METADATA` lands with the routing processors in Phase 5; `RAW_BYTES` is deferred until the Phase 0 benchmarks show protobuf parsing itself is a bottleneck, with the rule below recorded as its design.

Payload modes must have explicit contracts for item counting, routing metadata extraction, partial success, mutation, fanout copying, and batch merge. `RAW_BYTES` batching, when built, is a per-signal descriptor capability with a concrete wire-format basis: protobuf concatenation merges serialized traces, metrics, and logs export requests correctly because their only top-level field is repeated, while profiles are excluded because the singular dictionary field merges recursively and corrupts dictionary indices. Item counting and sizing over raw bytes require a streaming scan; a descriptor that does not supply them limits `RAW_BYTES` to same-protocol, no-mutation, no-batch forwarding.

### Service Configuration Model

Use a Collector-like configuration object:

```java
public record GatewayConfig(
        Map<ComponentId, ReceiverConfig> receivers,
        Map<ComponentId, ProcessorConfig> processors,
        Map<ComponentId, ExporterConfig> exporters,
        Map<ComponentId, ConnectorConfig> connectors,
        Map<ComponentId, ExtensionConfig> extensions,
        Map<PipelineId, PipelineConfig> pipelines,
        ServiceOptions service) {}

public record PipelineConfig(
        List<ComponentId> receivers,
        List<ComponentId> processors,
        List<ComponentId> exporters) {}
```

`PipelineId` should be signal plus optional name, matching the Collector mental model: `traces`, `traces/high-priority`, `metrics`, `logs/audit`, and so on.

### Runtime Construction Order

The graph builder should materialize the declared topology as a JGraphT `DirectedAcyclicGraph` of component nodes before constructing any component:

1. Resolve component IDs through a registry.
2. Validate that every referenced component exists.
3. Validate signal support for every receiver, processor, exporter, and connector pair as edge metadata.
4. Reject cycles at edge insertion: the DAG insertion rule makes connector cycles unrepresentable instead of checked after the fact.
5. Build connectors as graph edges between exporting and receiving pipelines.
6. Insert fanout nodes where multiple downstream consumers exist.
7. Instantiate components in reverse topological order, so exporters exist before the processors that feed them and receivers are built last.
8. Start in reverse topological order, receivers last.
9. Shut down in topological order, receivers first, draining queues into still-open exporters and extensions.
10. Roll back on start failure: a component that fails to start aborts startup and shuts down the already-started components in reverse start order, so a half-started gateway never serves.

One `TopologicalOrderIterator` walked in both directions replaces `Wiring.validate`, `Wiring`'s three hand-ordered shutdown phases, and most of `PipelineLifecycle`.

## Third-Party Library Integration

### Graph Modeling

| Candidate | Fit | Recommendation |
| --- | --- | --- |
| JGraphT (`org.jgrapht:jgrapht-core`) | Mature, dependency-light graph library. `DirectedAcyclicGraph` rejects cycle-creating edges at insertion, `TopologicalOrderIterator` gives deterministic construction/start/shutdown order, and cycle/connectivity inspectors give precise validation diagnostics. Dual-licensed EPL-2.0/LGPL-2.1. JPMS caveat: it ships as an automatic module (`org.jgrapht.core`), acceptable only because it never leaves `otlp4j-service`. | Baseline library for the internal service-graph model, adopted in the initial refactoring step. |
| Hand-rolled adjacency model | The Collector hand-rolls its graph over gonum primitives; a gateway graph is tens of nodes and topological sort is small code. | Fallback if the automatic-module dependency proves unacceptable for the JPMS story. The swap must be invisible: no graph library type in any exported package. |

Decision:

- Adopt JGraphT for the service graph in the initial refactoring step, wrapped behind an internal `ServiceGraph` type.
- Validation reports otlp4j diagnostics, never JGraphT exceptions.
- Hand-write the DOT-style `describe` output instead of adding `jgrapht-io`.

### Transport Runtime

| Candidate | Fit | Recommendation |
| --- | --- | --- |
| Armeria | One async HTTP/gRPC runtime on Netty, supports gRPC over HTTP/1 and HTTP/2, gRPC-Web, generated protobuf stubs, decorators, retrying clients, circuit breakers, metrics decorators, service decorators, and graceful shutdown. | Decided runtime for `otlp4j-transport-otlp`, implemented in Phase 4. The phase opens with a validation spike covering dependency footprint, JPMS impact, TLS/mTLS parity, body limits, backpressure behavior, operational hooks, and benchmarks; a failed spike is the only thing that reopens this decision. |
| Helidon SE 4 | Virtual-thread HTTP server and WebClient, gRPC server/client support, and shared HTTP/gRPC deployment on one WebServer. | Dropped. Keeping a second candidate warm is exactly the kind of undecided path this report closes, and the virtual-thread ergonomics it offers already exist at the synchronous transport send boundary. |
| grpc-java + Netty | Mature gRPC implementation already in use. | Remains the gRPC implementation until Phase 4 replaces it; deleted with the split transports once the Armeria module passes the conformance suite. |
| JDK `HttpServer` / `HttpClient` | Minimal dependencies and easy embedding. | Never the production transport. Retired to a test fixture in `otlp4j-testing` when Phase 4 lands; no separate no-dependency profile is maintained. |

Decision:

- Armeria is the transport runtime for `otlp4j-transport-otlp`, serving OTLP/HTTP and OTLP/gRPC from one module behind the transport SPI, with retry and timeout implemented by its built-in decorators.
- The no-Netty HTTP posture ends: the gRPC module already ships Netty into every dual-protocol deployment, so the split buys an isolation no realistic gateway deployment enjoys.
- Phase 4 opens with a time-boxed validation spike; if the spike fails, the decision reopens in this report with the findings recorded. No parallel alternative is kept warm.

### Resilience and Exporter Helpers

| Candidate | Fit | Recommendation |
| --- | --- | --- |
| Resilience4j | Already present. Provides retry, circuit breaker, rate limiter, bulkhead, time limiter, and composable decorators. | Removed from public API in Phase 1 and from the dependency graph in Phase 4. Until then it stays untouched inside the legacy transports' shared retry helper. |
| Armeria retry/circuit breaker decorators | Native to Armeria clients. | The decided implementation of `RetryPolicy`, `BackoffPolicy`, and `TimeoutPolicy` in `otlp4j-transport-otlp`: a retrying client whose rule folds `Retry-After`, gRPC pushback, and `RetryInfo`, with response timeouts covering per-attempt and total deadlines. HTTP and gRPC share one implementation because both run on one client runtime. |
| Failsafe | Lightweight, zero-dependency fault-tolerance library with concise retry/circuit-breaker policies. | Rejected. One resilience library is enough, and Resilience4j is already in the dependency graph. |

Decision:

- Introduce neutral policies in public API.
- The transport implements them: `otlp4j-transport-otlp` translates `RetryPolicy` and `BackoffPolicy` into an Armeria retrying-client rule and `TimeoutPolicy` into Armeria response timeouts, so retry, backoff, and pushback folding are owned by the runtime that already sees every response.
- Resilience4j is removed: it survives only inside the legacy transports until Phase 4 deletes them, then exits the dependency graph entirely. Armeria's circuit-breaker decorators stay off; the batch delivery queue owns refusal behavior.

### Queues and Concurrency

| Candidate | Fit | Recommendation |
| --- | --- | --- |
| JDK virtual threads | Already used by transports and fanout. Good for blocking integration boundaries. | Keep, but bind them through runtime executors and bulkheads instead of ad hoc per-operation executors. |
| JCTools | Lock-free SPSC/MPSC/SPMC/MPMC queues and batch drain/fill APIs. | Rejected for this milestone; revisited only if the Phase 0 baseline shows the queue is a bottleneck. |
| JDK `ArrayBlockingQueue` | Simple bounded queue used by `Batcher`. | The decided queue. The internal queue policy abstraction wraps it so a future swap stays invisible. |

Decision:

- `ArrayBlockingQueue` is the queue; the redesign contains no queue-replacement work.
- Queue policy stays separate from queue implementation and out of public API.
- Only Phase 0 baseline evidence of a queue bottleneck reopens the question.

### Serialization and Protobuf

protobuf-java should remain the default serialization library. The problem is not protobuf-java; the problem is otlp4j always materializing the domain model and byte arrays.

Recommendations:

- Use `MessageLite.getSerializedSize()` for accounting.
- Prefer `MessageLite.writeTo(OutputStream)` or transport-native buffers over `toByteArray()` where possible.
- Parse from bounded streams or transport buffers with size limits.
- Avoid converting trace/span IDs to hex strings unless a domain view is explicitly requested.
- Make payload materialization lazy and cache views only when safe.

## Module-by-Module Refactoring Blueprint

### `otlp4j-api`

P0:

- Split public contracts from runtime implementations.
- Remove the `requires transitive io.github.resilience4j.retry` and `io.github.resilience4j.core` declarations from `module-info.java` along with the `RetryConfig` record component that forces them.
- Introduce component IDs, pipeline IDs, factories, component lifecycle, consumer capabilities, signal descriptors, neutral retry/backoff policy records, and typed delivery errors.
- Move `Pipeline`, `Batcher`, `Connector`, `Wiring`, `FanOut`, and lifecycle discovery into runtime or embedding modules.

P1:

- Ship no type aliases or adapters; zero blast-radius constraints make the clean break the plan, not a fallback.
- Replace `Source` and `Sink` as primary gateway concepts with `Receiver` and `SignalConsumer`.

### `otlp4j-model`

P0:

- Stop using `Signal` as the runtime registry for built-in and experimental OTLP signals.
- Move merge, item-count, and empty-batch behavior to `SignalDescriptor` or `PayloadCodec`.
- Keep model records as immutable domain views.

P1:

- Add no-flatten iteration APIs consistently to reduce allocations.
- Keep expensive convenience flattening methods out of hot paths.
- Keep hex-string IDs in the domain records. The records are a view, and lazy materialization removes them from the hot path, so a byte-level ID representation is complexity without a payoff.

### `otlp4j-codec`

P0:

- Replace mapper-only design with payload codecs:
  - parse wire request to `TelemetryPayload`
  - materialize domain view lazily
  - write payload directly to transport
  - build protocol response from `DeliveryResult`
- Build no raw-byte payload mode; it is deferred behind benchmark evidence, with its merge rules recorded under Payload Representation.

P1:

- Add byte and item sizers per signal.
- Add tests proving pass-through pipelines do not force domain materialization.
- Keep generated proto exports qualified to the codec and transport modules, as the current `module-info` already does.

### `otlp4j-transport-common`

P0:

- Split into:
  - transport SPI: receiver sender/client/server contracts
  - exporter runtime: exporter metrics and `DeliveryResult` mapping (landing in `otlp4j-service`); retry and timeout belong to the transport
  - receiver runtime: admission, protocol response mapping, telemetry tap

P1:

- Convert `ClientExporter` into an exporter component factory result.
- Convert `ServerReceiver` into a receiver component factory result.
- Normalize thrown subscriber exceptions before transport-specific response mapping.

### `otlp4j-transport-http`

P0:

- Replace `com.sun.net.httpserver.HttpServer` with the Armeria transport in Phase 4; until then it stays untouched apart from the P1 hardening below.
- Implement OTLP/HTTP as a component factory under the same service graph as gRPC.

P1:

- Move admission byte checks before full decoded-body allocation when protocol metadata permits.
- Keep the existing decoded body cap as a safety net.
- Support explicit request, header, idle, and shutdown timeouts.
- Centralize decompression policy and supported encodings.
- Avoid always buffering export payloads as `byte[]`.

### `otlp4j-transport-grpc`

P0:

- Wrap the module as a component factory under the service graph in Phase 1, swapping its public config to the neutral policy records (translated internally to the existing `ResilienceRetries` helper).
- No other restructuring: Phase 4 deletes the module, so moving its retry, timeout, or circuit-breaker code around first is wasted work.

P1:

- Keep blocking stubs on virtual threads until Phase 4 deletes the module; no interim stub migration.
- Map retry pushback into the neutral `RetryAfter` policy, and close the interop gap in throttling signaling: the current implementation emits and consumes only the `grpc-retry-pushback-ms` trailer, while the OTLP specification signals throttling through `RetryInfo` in `grpc-status-details-bin`, so an otlp4j exporter misses the retry hint from a spec-compliant Collector and a spec-compliant client misses otlp4j's.
- Treat Netty configuration as transport implementation config, not core API.

### `otlp4j-proto`

P1:

- Keep as generated schema module.
- Expose enough descriptors/parsers for codecs without forcing proto classes into high-level API.
- Add version/alignment checks against the vendored proto source.

### `otlp4j-testing`

P0:

- Build the wire-level OTLP conformance suite and the JMH baseline first (Phase 0). They are API-agnostic and survive the redesign; the component conformance list below targets the new graph contracts and lands with them.

P1:

- Add conformance tests for:
  - graph validation failures
  - lifecycle start/shutdown order
  - connector topology
  - fanout result merging
  - retryable versus permanent failures
  - admission refusal
  - lazy payload materialization
  - exporter queue overflow

### `otlp4j-samples`

P2:

- Replace most samples with service-graph examples.
- Keep one embedding sample for users who only need a small in-process pipeline.

## Concrete User-Facing API Proposals

### `Pipeline.java`

Current role:

- Fluent builder from `Source<T>` to terminal `Sink`.
- Owns sync/async transform composition.
- Detects terminal lifecycle resources.
- Supports `via(Processor<T>)` and `owns(AutoCloseable)`.

Proposed role:

- Optional embedding adapter, not gateway runtime.
- It compiles to a `GatewayConfig` consumed by the standard graph builder; there is no second construction path.
- It should not perform terminal shape classification.
- It should not be responsible for lifecycle discovery.

Recommended replacement:

```java
Gateway gateway = Gateway.builder()
        .receiver("otlp", OtlpReceiver.config().http().grpc())
        .processor("batch", BatchProcessor.config().maxItems(512).timeout(Duration.ofSeconds(1)))
        .exporter("otlp/out", OtlpExporter.config().endpoint("https://backend:4318"))
        .pipeline("traces", p -> p.receivers("otlp").processors("batch").exporters("otlp/out"))
        .build();
```

For embedded use:

```java
EmbeddedPipeline.traces()
        .from(receiver)
        .via(processor)
        .to(exporter)
        .start();
```

The embedded API should use the same runtime contracts as service mode.

### `Batcher.java`

Current role:

- Processor and sink.
- Queue and drain engine.
- Scheduler ownership.
- Ack mode.
- Circuit breaker and timeout.
- Metrics and lifecycle.

Proposed replacement:

- `BatchProcessor`: graph processor that groups signal payloads by count, bytes, timeout, and optional metadata partition keys.
- A delivery queue helper: reusable runtime internals for bounded delivery, optional wait-for-result semantics, and the existing degraded-window ingress refusal.
- No `SendingQueue`: the delivery queue is the single queueing point in the pipeline, and a separate exporter-side queue returns only with persistent storage, which is a non-goal.

Public config example:

```java
public record BatchPolicy(
        int sendBatchSize,
        int sendBatchMaxSize,
        long sendBatchMaxBytes,
        Duration timeout,
        List<String> metadataKeys) {}

public record DeliveryQueuePolicy(
        int queueSize,
        int numConsumers,
        OverflowPolicy overflow,
        boolean waitForResult) {}
```

Remove `AckMode`; wait-for-result behavior is `DeliveryQueuePolicy.waitForResult`. Persistent storage is a non-goal.

### `Connector.java`

Current role:

- Abstract sink that derives zero/one output batch from one input batch.
- Supports downstream fanout and best-effort/fail policy.

Proposed replacement:

- `ConnectorFactory` with declared signal pairs.
- `ConnectorComponent` that is both consumer on the input side and receiver/router on the output side.
- `DerivingConnector` helper for simple count, span-to-metric, log-to-metric, or routing cases.

Validation:

- The graph must reject a connector referenced as an exporter without a compatible receiver pipeline.
- The graph must reject a connector referenced as a receiver without a compatible exporter pipeline.
- The graph must detect connector cycles.

### `Wiring.java`

Current role:

- Bundles receivers, pipeline handles, and owned resources.
- Applies shutdown order and validation heuristics.

Proposed replacement:

- `GatewayRuntime` produced by `GatewayBuilder` over the internal JGraphT-backed `ServiceGraph`.
- `GatewayRuntime.start()` starts components in reverse topological order, receivers last.
- `GatewayRuntime.shutdown()` drains in topological order with deadlines, receivers first.
- Validation runs before any component starts.

`Wiring` is deleted. `otlp4j-embedding` builds on `GatewayConfig` and needs nothing from it.

## Hardening Plan

### Error Boundaries

Action items:

- Wrap every component invocation in a graph boundary that converts exceptions to `DeliveryResult`.
- Preserve cause, component ID, component kind, signal, retryability, retry-after, and downstream provenance.
- Make protocol adapters pure mappers from `DeliveryResult` to OTLP HTTP/gRPC response.
- Add tests for sync exceptions, async failed stages, fanout mixed results, timeout, shutdown, admission refusal, and downstream exporter refusal.

### Backpressure and Admission Control

Action items:

- Move admission from receiver implementation detail to a service-level component.
- Count in-flight requests, in-flight serialized bytes, queued payloads, queue bytes, and runtime memory pressure.
- Add a memory limiter with soft and hard thresholds.
- Let processors/exporters declare bounded concurrency.
- Ensure receiver refusal is retryable and carries retry-after when configured.
- Use gRPC pushback and HTTP `Retry-After` consistently.

### Memory Footprint During Marshalling

Action items:

- Introduce `TelemetryPayload` before replacing transports.
- Convert HTTP export from `toByteArray()` to direct write or buffer wrapping.
- Delay model materialization until a processor requests `DOMAIN_IMMUTABLE`.
- Cache materialized views carefully and invalidate/copy when mutation is declared.
- Keep raw protobuf request where processors are pass-through.
- Add allocation-sensitive benchmarks for receive-forward-export.

### Concurrency Safety

Action items:

- Replace ad hoc virtual-thread spawning in fanout and transports with runtime-managed executors bounded by `ExecutionPolicy`.
- Add cancellation/deadline to `ConsumeContext`.
- Require processors to declare mutation capability and thread-safety expectations.
- For immutable payload mode, fanout can share payloads.
- For mutable payload mode, the graph inserts a copy before each mutating consumer (the Collector's fanout rule); topology is never rejected for mutation, because copying makes it safe.

### Observability

Action items:

- Keep `GatewayMetrics` as the self-observability SPI; ship an OpenTelemetry API adapter implementation and keep the existing no-op and logging implementations for tests. Replacing the SPI would break every instrumented component for no gain.
- Emit component-level metrics:
  - accepted/refused items
  - queue depth and queue bytes
  - retry attempts
  - dropped items
  - batch size and flush reason
  - export latency
  - memory limiter state

## Development Workstream Guardrails

These rules govern how the phases land, not what they contain.

- Land on trunk behind the conformance suite. The graph runtime is built beside the fluent API, both wrapping the same transports, and the fluent path is deleted only when the graph runtime passes the same wire-level suite. A long-lived redesign branch would rot against the transports and codecs it has to keep absorbing.
- The OTLP wire contract is the only interface that survives the redesign. Cross-phase tests target the wire surface plus one thin gateway bootstrap seam; tests written against the fluent API die with it and prove nothing across the break.
- Version the redesign as 0.x with per-component stability levels until Phase 3 exits. Factories declare development/alpha/beta/stable so profiles support and new components ship without implying stability the project cannot honor yet.
- Non-goals for the first milestone: arbitrary third-party signal registration, persistent sending queues, raw-byte payload mode, config hot-reload, OTLP/HTTP JSON encoding, and multi-tenant isolation. The codec and config seams must not preclude them, but no phase spends effort on them.

## Priority Roadmap

### Phase 0: Conformance and Performance Baseline

Priority: P0

The redesign deletes every internal interface a test could target, so the baseline must be written against the one contract that survives: the OTLP wire protocol. This phase precedes all refactoring.

Deliverables:

- A black-box OTLP conformance suite in `otlp4j-testing` that drives a running gateway over HTTP and gRPC: status mapping, partial success, retry-after and pushback trailers, gzip, TLS/mTLS, oversized payloads, and admission refusal.
- Codec round-trip property tests (proto to domain to proto) pinning mapper behavior before lazy materialization lands.
- A JMH benchmark module capturing receive-forward-export throughput and allocation on the current tree.

Exit criteria:

- The suite passes against the current fluent runtime and touches no fluent API type outside one bootstrap seam.
- Baseline throughput and allocation numbers are recorded, so Phase 2 efficiency claims are measured against them rather than asserted.

### Phase 1: Service Graph Foundation and API Break

Priority: P0

This phase aggregates every graph-shaped change in this report into one refactoring unit: the graph runtime, connector topology, declared lifecycle ordering, and the `Wiring`/`PipelineLifecycle` replacement land together.

Deliverables:

- `otlp4j-core` contracts.
- `otlp4j-service` graph builder and runtime on an internal JGraphT `DirectedAcyclicGraph`, with topological start/shutdown ordering.
- Component registry.
- Pipeline ID and component ID model.
- Connector-as-graph-edge model with declared signal pairs.
- Extension component kind in the registry and graph lifecycle (contract only; the first concrete extensions land in Phase 5).
- Graph validation: missing references, signal mismatches, connector pair compatibility, and cycle rejection at edge insertion.
- Neutral retry/backoff policy records.
- Delivery provenance plus typed retryable, permanent, and downstream errors.
- Current OTLP receiver/exporter wrapped as factories.
- Deletion of `Wiring` and terminal-shape lifecycle discovery.

Exit criteria:

- A traces pipeline can be built from service config and started/stopped by graph runtime.
- Invalid component references, signal mismatches, incompatible connector pairs, and connector cycles fail before startup.
- Start and shutdown order is derived from the graph's topological order, not registration order.
- Existing fluent samples can be rewritten without `Wiring`.
- No JGraphT type appears in any exported package.

### Phase 2: Payload and Codec Redesign

Priority: P0

Deliverables:

- `TelemetryPayload`.
- Built-in OTLP signal descriptors.
- Lazy domain model materialization.
- Direct protobuf write helpers.
- Protocol response mapper from `DeliveryResult`.
- Payload-mode contracts for item counting, merge, mutation, fanout copying, and partial success.

Exit criteria:

- Same-protocol no-mutation OTLP HTTP and gRPC pass-through pipelines do not materialize `TracesData` unless a processor requests it.
- Shipped payload views are `PROTO_REQUEST` and `DOMAIN_IMMUTABLE`; raw bytes stay deferred behind benchmark evidence.
- Export can account serialized size without byte-array materialization.

### Phase 3: Exporter Runtime

Priority: P1

The exporter runtime is thin by design: retry and timeout are implemented by the transport (Armeria's built-in decorators, arriving in Phase 4), so the shared helper owns only what is transport-independent.

Deliverables:

- Exporter metrics wrapper around the transport send boundary.
- Executable semantics tests for `RetryPolicy`, `BackoffPolicy`, and `TimeoutPolicy`: attempt counts, backoff bounds, retry-after and pushback folding, and deadline behavior, run against the legacy transports now and gating the Armeria transport in Phase 4.
- Interim translation of the neutral policies to the legacy clients' existing `ResilienceRetries` helper.

Exit criteria:

- Exporters accept only neutral policies; public API contains no Resilience4j types.
- No sending queue, circuit breaker, or bulkhead ships; the batch delivery queue and its degraded-window refusal remain the only queueing and breaking points.
- The policy semantics suite passes on the legacy transports and is ready to gate Phase 4.

### Phase 4: Transport Consolidation

Priority: P1

Deliverables:

- Opening validation spike: dependency footprint, JPMS/module impact, TLS/mTLS parity, operational hooks, and benchmarks against the Phase 0 baseline.
- Armeria-based `otlp4j-transport-otlp` receiver and exporter factories serving both protocols.
- Translation of `RetryPolicy`, `BackoffPolicy`, and `TimeoutPolicy` to Armeria's retrying-client rules and response timeouts, folding `Retry-After`, gRPC pushback, and `RetryInfo`.
- Equivalent HTTP and gRPC protocol mapping tests: graceful shutdown, compression, body limits, retry-after, pushback, and `RetryInfo` mapping.

Exit criteria:

- The Phase 0 conformance suite, the Phase 3 policy semantics suite, and the benchmarks pass on the Armeria transport at parity or better.
- The JDK HTTP and grpc-java transport modules are deleted, taking the hand-rolled retry code and Resilience4j out of the dependency graph; the JDK HTTP server survives only as a test fixture in `otlp4j-testing`.
- A failed spike is the only outcome that reopens the transport decision, with the findings recorded in this report.

### Phase 5: Built-In Components and Declarative Config

Priority: P2

Deliverables:

- Batch processor.
- Memory limiter.
- Routing/filtering processors.
- Deriving connector helper.
- Authenticator extension SPI: server-side authenticators attachable to receivers and client-side credentials on exporters, following the Collector extension model. A gateway framework without auth hooks is not deployable at an ingest edge, and the extension component kind reserved in Phase 1 is where they live.
- YAML config loader mirroring the Collector configuration structure, so Collector users can port pipeline definitions with minimal translation.
- Component conformance test kit.

Exit criteria:

- A Collector-like service config can express receiver -> processors -> exporter pipelines and connector-mediated pipelines.
- Third-party components can be added without touching core modules.

## Definitive Recommendations by Impact

1. Rebuild around a service graph modeled internally on JGraphT, and land all graph-shaped work (registry, validation, connector topology, lifecycle ordering, `Wiring` replacement) as the initial refactoring step. This is the highest-impact change and should precede API polish.
2. Decouple signal runtime metadata and payload representation from the immutable model. A gateway cannot stay efficient while model batches are mandatory.
3. Move queue, batching, admission, and lifecycle to runtime helpers, and retry and timeout to the transport runtime's built-in support. They are gateway infrastructure, not pipeline DSL details.
4. Remove Resilience4j from public API in Phase 1 and from the dependency graph in Phase 4. The neutral policies are implemented by Armeria's retrying client and response timeouts, not by a custom retry layer.
5. Adopt Armeria as the OTLP transport runtime, gated only by the Phase 4 opening spike. The no-Netty HTTP posture ends because the gRPC module already ships Netty into every dual-protocol deployment.
6. Keep the current immutable model as a user-facing view. Do not make it the mandatory internal representation for receive-forward-export.
7. Replace `Connector<I,O>` with graph connectors. Simple deriving connectors should be helpers on top of the graph model.
8. Replace `Wiring` with `GatewayRuntime`. Lifecycle should be graph-derived and validated before startup.
9. Write the wire-level conformance suite and performance baseline before any refactoring lands, and add graph conformance tests before optimizing queues. Correct wire behavior, graph behavior, error boundaries, and backpressure semantics matter more than the first queue implementation, and Phase 2's efficiency claims need a measured baseline.

## Evidence Paths

Current otlp4j files:

- `pom.xml`
- `otlp4j-api/src/main/java/module-info.java`
- `otlp4j-api/src/main/java/dev/nthings/otlp4j/config/ClientConfig.java`
- `otlp4j-api/src/main/java/dev/nthings/otlp4j/pipeline/Pipeline.java`
- `otlp4j-api/src/main/java/dev/nthings/otlp4j/pipeline/Wiring.java`
- `otlp4j-api/src/main/java/dev/nthings/otlp4j/pipeline/ops/Batcher.java`
- `otlp4j-api/src/main/java/dev/nthings/otlp4j/pipeline/ops/Connector.java`
- `otlp4j-model/src/main/java/dev/nthings/otlp4j/model/Signal.java`
- `otlp4j-model/src/main/java/dev/nthings/otlp4j/model/ConsumeResult.java`
- `otlp4j-codec/src/main/java/dev/nthings/otlp4j/codec/SignalCodec.java`
- `otlp4j-codec/src/main/java/dev/nthings/otlp4j/codec/CommonMapper.java`
- `otlp4j-transport-common/src/main/java/dev/nthings/otlp4j/transport/common/AdmissionControl.java`
- `otlp4j-transport-common/src/main/java/dev/nthings/otlp4j/transport/common/ResilienceRetries.java`
- `otlp4j-transport-common/src/main/java/dev/nthings/otlp4j/transport/common/ClientExporter.java`
- `otlp4j-transport-common/src/main/java/dev/nthings/otlp4j/transport/common/ServerReceiver.java`
- `otlp4j-transport-common/src/main/java/dev/nthings/otlp4j/transport/common/SignalSource.java`
- `otlp4j-transport-http/src/main/java/dev/nthings/otlp4j/transport/http/internal/HttpOtlpServer.java`
- `otlp4j-transport-http/src/main/java/dev/nthings/otlp4j/transport/http/internal/HttpOtlpClient.java`
- `otlp4j-transport-http/src/main/java/dev/nthings/otlp4j/transport/http/internal/HttpExchangeHandlers.java`
- `otlp4j-transport-grpc/src/main/java/dev/nthings/otlp4j/transport/grpc/internal/GrpcOtlpServer.java`
- `otlp4j-transport-grpc/src/main/java/dev/nthings/otlp4j/transport/grpc/internal/GrpcOtlpClient.java`
- `otlp4j-transport-grpc/src/main/java/dev/nthings/otlp4j/transport/grpc/internal/GrpcServiceAdapters.java`

Local upstream references:

- `$HOME/gitbase-data/open-telemetry/opentelemetry-java/sdk/common/src/main/java/io/opentelemetry/sdk/common/CompletableResultCode.java`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-java/sdk/trace/src/main/java/io/opentelemetry/sdk/trace/export/SpanExporter.java`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-java/sdk/metrics/src/main/java/io/opentelemetry/sdk/metrics/export/MetricExporter.java`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-java/sdk/logs/src/main/java/io/opentelemetry/sdk/logs/export/LogRecordExporter.java`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-java/sdk/trace/src/main/java/io/opentelemetry/sdk/trace/export/BatchSpanProcessor.java`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/component/component.go`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/receiver/receiver.go`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/processor/processor.go`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/exporter/exporter.go`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/connector/connector.go`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/pipeline/pipeline.go`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/consumer/consumer.go`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/consumer/consumererror`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/service/internal/graph`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/processor/batchprocessor`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/processor/memorylimiterprocessor`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/exporter/exporterhelper`
- `$HOME/gitbase-data/open-telemetry/opentelemetry-collector/receiver/otlpreceiver`

Third-party documentation:

- Armeria overview: https://armeria.dev/docs/
- Armeria retrying clients: https://armeria.dev/docs/client/retry/
- Armeria service decorators: https://armeria.dev/docs/server/decorator/
- Armeria graceful shutdown: https://armeria.dev/docs/server/graceful-shutdown/
- Helidon WebServer: https://helidon.io/docs/v4/se/webserver/webserver
- Helidon gRPC server: https://helidon.io/docs/v4/se/grpc/server
- JGraphT user guide: https://jgrapht.org/guide/UserOverview
- Resilience4j: https://github.com/resilience4j/resilience4j
- Failsafe: https://failsafe.dev/
- JCTools: https://github.com/JCTools/JCTools
- protobuf-java `MessageLite`: https://protobuf.dev/reference/java/api-docs/com/google/protobuf/MessageLite
