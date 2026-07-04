# otlp4j Architecture & API Review

> Evaluation of otlp4j's design and structure as a foundation for an extensible OpenTelemetry gateway (a JVM counterpart to the OpenTelemetry Collector). Scope: the `0.1.0-SNAPSHOT` source tree at the time of writing, covering `otlp4j-model`, `-api`, `-codec`, `-proto`, `-transport-common`, `-transport-grpc`, and `-transport-http`. References to upstream concepts use the OpenTelemetry Collector (`go.opentelemetry.io/collector`, stable module set v1.61.0 / beta v0.155.0) and the OpenTelemetry Java SDK abstractions (1.64.0).
>
> Method: source-only review, read without reference to prior plans, notes, or git history. Every claim is grounded in code; file and class names are concrete so changes are actionable. Breaking changes are in scope: module, package, and class renaming, plus additions and removals, are all on the table.

---

## 1. Executive summary

otlp4j is, on the whole, a clean and well-disciplined design. The headline strengths are real and uncommon in early-stage Java telemetry libraries:

- A strict one-way dependency graph (`model` then `codec` then `transports`; `api` then `transports`) enforced by JPMS modules, with generated proto confined to qualified exports so no `com.google.protobuf.*` or gRPC type ever leaks into the user-facing surface.
- An immutable-records domain model (`otlp4j-model`, about 22 files) that mirrors the OTLP hierarchy and validates at construction time, with honest treatment of the hard cases (opaque `profiles` passthrough, `Metric.NoData` for `DATA_NOT_SET`).
- A `ConsumeResult` sealed type that fuses OTLP partial-success semantics (`Accepted` / `Partial` / `Rejected`) with retry intent and a `retryAfter` hint, and is mapped identically by gRPC and HTTP into transport-level pushback. This is the single best idea in the codebase and the load-bearing contract for everything above it.
- A genuinely transport-agnostic shared tier (`otlp4j-transport-common`) that owns lifecycle, reference-counting, admission control, the tap, and retry plumbing. gRPC and HTTP are thin SPI adapters plus entry points, so a third transport would be cheap to add.

Measured against the OpenTelemetry Collector, the explicit north star, otlp4j lands in a good but unfinished place. It has analogues for the Collector's three core ideas (a decoupled data model, a uniform consumer-style contract, and signal-typed component interfaces), and in two respects it improves on the Collector: immutability (Collector's `pdata` is mutable, which forces the `fanoutconsumer` to clone-before-mutate based on a `Capabilities.MutatesData` flag; otlp4j's records make that entire problem disappear, since `FanOut` shares one immutable reference safely), and an async, deadline-bounded contract (Collector's `ConsumeTraces` returns `error` synchronously; otlp4j's `CompletionStage<ConsumeResult>` lets a slow pipeline hold the OTLP request open with a real timeout, which the Collector achieves only by spawning goroutines and ad-hoc contexts).

The gaps that prevent otlp4j from being a simple yet robust gateway foundation today are not in any single module. They sit at the seams between layers and in the vocabulary:

1. Vocabulary overload and fragmentation. The words processor, sink, source, transform, stage, connector, and lifecycle are each doing two or three jobs across `pipeline/` and `processor/`, and the four single-method interfaces (`Transform`, `Processor`, `Sink`, `Connector`) spread one conceptual matrix (sync vs async, value vs result, same-signal vs derived) across two packages. This is the largest barrier to the "simple" goal.
2. No first-class Processor component. The Collector's central abstraction, a user-extensible, signal-typed, stateful processor with a `next` consumer, has no real equivalent. `BatchingProcessor` is misnamed (it is a buffering sink, not a `Processor`); `Transforms` are stateless one-to-one maps; the signal-changing path (`Connector`) is hardcoded to two count connectors. A gateway framework lives or dies by how easy it is to write one new processing stage, and here that path is underspecified.
3. Per-signal quadruplication. Traces, metrics, logs, and profiles are enumerated four times over: four `*Sink` marker interfaces, four `*Data` batch records, four codec mappers, four `export*` / `dispatch*` methods, four gRPC adapters, four HTTP contexts, four `Signal` enum entries. There is no `Signal`-keyed generic spine, so adding a signal or a processor "for all signals" is O(signals) boilerplate.
4. Lifecycle is powerful but implicit. The `SharedLifecycle` reference-counting, structural `AutoCloseable` collection, the `Stage.owns()` escape hatch, and `Wiring` phase ordering are correct and even elegant, but they are invisible. There is no ownership graph a user can see or that the framework validates, and the "lambda hides a resource" failure mode is delegated to the user.

None of these is fatal; all are fixable with focused, mostly local refactors. Section 5 prioritizes them; Section 6 lays out a phased path. The verdict: otlp4j has the right bones to become a robust, extensible gateway foundation, and the immutability and async `ConsumeResult` decisions give it a genuine structural advantage over the Collector's mutable-`pdata` model. Reaching that potential requires collapsing the vocabulary, introducing a first-class Processor component, and de-duplicating the per-signal fan-out.

---

## 2. What the framework gets right (keep these)

| Area | Why it is good |
| --- | --- |
| Module boundaries | `model` then `codec` then `transports`, and `api` then `transports`. Proto is qualified-exported only to `codec` plus the two transports. A consumer of `otlp4j-api` never sees a generated type. This is exactly the Collector's `pdata` decoupling goal, achieved via JPMS rather than a generated wrapper layer. |
| `ConsumeResult` as the universal contract | `sealed interface ConsumeResult permits Accepted, Partial, Rejected` (in `otlp4j-model`) maps one-to-one to OTLP partial-success and carries retry intent and `retryAfter`. Both transports encode it consistently: `codec.SignalResponses` turns `Accepted` / `Partial` into the four proto responses, `codec.DeliveryResults` turns `Rejected` into HTTP 503 / 500 plus `Retry-After`, and the gRPC adapters emit the `grpc-retry-pushback-ms` trailer. One contract, end to end. |
| Immutability | The domain hierarchy is records with `List.copyOf` and defensive copies in compact constructors (sealed interfaces where variants demand them: `AttributeValue`, `ConsumeResult`; `Attributes` is a final class). IDs are validated at construction (`Ids.traceId` / `spanId` / `parentSpanId`), not on the export hot path. `FanOut` can broadcast one reference with no clone, a class of bug the Collector spends real machinery (`Capabilities.MutatesData` plus `fanoutconsumer` clone-on-mutate) to manage. |
| Async, deadline-bounded sinks | `Sink<T>::consume(T)` returns `CompletionStage<ConsumeResult>`. A slow pipeline naturally holds the request; `Pipeline` lifts sync `transform` / `filter` into a zero-allocation fast path and only introduces futures at a `process` boundary. This is a stronger contract than the Collector's synchronous `ConsumeTraces(ctx, td) error`. |
| Transport-common shared tier | `ClientExporter`, `ServerReceiver`, `AdmissionControl`, `ReceiverTap`, `MulticastPublisher`, the two abstract builders, `ClientHeaders`, and `ResilienceRetries` are all transport-agnostic. The gRPC and HTTP modules are thin: one SPI impl each (`*OtlpClient` / `*OtlpServer`), entry-point classes, and the per-signal adapters. A new transport is roughly "implement two interfaces and reuse everything." |
| Admission control | `AdmissionControl` (CAS, non-blocking) caps decoded requests and bytes in flight across every connection, with a deliberate progress guarantee (the 0-to-1 admit always wins, so one oversized request cannot starve). The core Collector offers transport-level caps (`max_request_body_size`, `max_recv_msg_size_mib`, `max_concurrent_streams`) and a global memory limiter, but no bounded admission over decoded in-flight requests and bytes; this hardening is otlp4j's own. |
| Honest scope | `@Experimental` on profiles; `mergeProfilesUnsafe` / `forProfilesUnsafe` named for the real (dictionary-reindexing) hazard; `ProfilesData.dictionary()` returns a defensive clone; server response compression is documented as deliberately absent. The code says what it cannot do. |

---

## 3. The Collector concept map (so the gaps are concrete)

The OpenTelemetry Collector is the named target, so it is worth pinning down the four abstractions that make it extensible, and where otlp4j stands relative to each. Sources: `consumer/consumer.go`, `component/component.go`, `receiver/receiver.go`, `exporter/exporter.go`, `processor/processor.go`, `processor/processorhelper/traces.go`, `internal/fanoutconsumer/traces.go`, `connector/connector.go`, `connector/traces_router.go`, `exporter/exporterhelper` (queue), `service/internal/graph`.

| Collector concept | What it is | otlp4j analogue | Gap |
| --- | --- | --- | --- |
| `pdata` (`ptrace` / `pmetric` / `plog`) | Mutable, in-memory, proto-backed data model; the only thing that travels through pipelines. Decoupled from wire by a private `orig` pointer. | `otlp4j-model` immutable records (`TracesData` / `MetricsData` / `LogsData` / `ProfilesData`). | None. otlp4j is strictly better here, since immutability removes the `MutatesData` problem. The only cost is merge and allocation overhead, discussed in section 4. |
| `consumer.Traces` (`ConsumeTraces(ctx, td) error`) | The universal contract: receivers, processors, exporters, and connectors all consume a signal. A "next consumer" is threaded through every stage. | `Sink<T>::consume(T)` returning `CompletionStage<ConsumeResult>`. Stronger (async plus typed result). | The concept exists, but the type taxonomy is fragmented (Sink / Transform / Processor / Connector), and there is no "next consumer" threading. Pipelines are a linear DSL terminated at a sink, not a chain of consumers. |
| `component.Component` (`Start` / `Shutdown` plus `Host`) | One lifecycle interface for all component kinds, with a `Host` whose entire surface is extension lookup (`GetExtensions()`). | `Lifecycle` (`shutdown(Duration)` / `forceFlush`) plus `SharedLifecycle` refcounting plus `Wiring`. | otlp4j's lifecycle is richer (deadline-bounded, refcounted) but component-kind-blind: there is no `Processor` component interface, no `Host` / capabilities, no uniform `Settings`. |
| `processorhelper` (`ProcessTracesFunc` plus `Capabilities`) | The canonical way to write a processor: a function `(ctx, td) -> (td, err)` plus a flag saying whether you mutate. Wires a `next` consumer automatically. | `pipeline.Processor<T>` (async one-to-one) plus `processor.Transforms` (stateless) plus `processor.BatchingProcessor` (buffering sink). | The biggest gap. No stateful, signal-typed, `next`-threaded processor abstraction. `BatchingProcessor` is a sink, not a `Processor`. Writing "filter, enrich, and forward" as a reusable component is not a first-class operation. |
| `fanoutconsumer` (clone-on-mutate) | Fans out to N consumers, cloning only for those that declare `MutatesData`. | `pipeline.FanOut` (shares one immutable reference, merges `ConsumeResult`s). | otlp4j is better (no clone needed), but `FanOut`'s "parallel" claim is overstated. Peers are launched sequentially on the caller thread (section 4). |
| `connector` (`TracesToMetrics`, and so on) | N-by-M signal-conversion component matrix, factory-wired; the service graph hands each connector a router (`connector.NewTracesRouter`) that fans out to every matching downstream pipeline. | `processor.Connector<I,O>` (abstract, one downstream, `derive(I) -> O`) plus `Connectors.spanCount` and `logRecordCount`. | The shape exists but is single-downstream and has exactly two built-ins. The matrix is not generalized, and there is no framework-provided fan-out to the downstream side. |

The OpenTelemetry Java SDK, the other upstream reference, confirms the result-contract advantage from the JVM side. Its `CompletableResultCode` is a binary success / failure carrying at most an optional throwable: no partial success, no retry intent, no retry-after hint. `ConsumeResult` is strictly more expressive than both upstream result types. The SDK's `BatchSpanProcessor` shares otlp4j's drop-on-overflow batching posture, but it self-instruments the queue (size, dropped, and processed counts, emitted as metrics), a precedent F11 returns to.

Net: otlp4j matches or beats the Collector on the data model and the async contract, and ties on component kinds, but it loses on the processor component, which is the thing a gateway framework is most used for.

---

## 4. Findings (prioritized)

Each finding lists what, evidence, why it matters, and recommendation. P0 blocks the "simple and extensible gateway foundation" goal; P1 should be fixed before 1.0; P2 is polish.

### F1 (P0). The vocabulary is overloaded and the four single-method interfaces fragment one concept

> **Status: resolved (Phase 0).** The renames in this finding have landed. `pipeline.Processor` is now `AsyncTransform` (its SAM method is `applyAsync`, and the pipeline DSL verb is `Stage.transformAsync`, mirroring `Transform.apply` and `Stage.transform`), the `dev.nthings.otlp4j.processor` package is now `dev.nthings.otlp4j.pipeline.ops`, and `BatchingProcessor` is now `Batcher`. The `Processor` name is deliberately left free for the future first-class component described in F2. The finding text below is preserved as the original review.

What. Four closely-related SAM types spread across `pipeline/` and `processor/` encode one conceptual matrix:

| Type | Package | Shape | Meaning |
| --- | --- | --- | --- |
| `Transform<T>` | `pipeline` | `T apply(T)`, sync | one-to-one same-signal rewrite |
| `Processor<T>` | `pipeline` | `CompletionStage<T> process(T)`, async | one-to-one same-signal async rewrite |
| `Sink<T>` | `pipeline` | `CompletionStage<ConsumeResult> consume(T)`, async | terminal; returns a result |
| `Connector<I,O>` | `processor` | `Sink<I>` plus `derive(I) -> O` | signal-changing |

On top of this, "processor" means three different things: the `pipeline.Processor` interface; the entire `processor/` package (which holds `BatchingProcessor`, a `Sink`, not a `Processor`, that cannot be plugged into `Stage.process(...)`); and the OpenTelemetry / Collector meaning of processor (a stateful pipeline component). `BatchingProcessor` being named `...Processor` while implementing `Sink`, not `Processor`, is the clearest symptom.

Evidence. `Pipeline.Stage` exposes `process(Processor<T>)` (the async one-to-one interface). `BatchingProcessor` `implements Sink<T>, Lifecycle` and is attached via `.to(...)` or `FanOut.of(...)`, never via `.process(...)`. A reader navigating from the `processor/` package to find the `Processor` type lands on `pipeline.Processor` and on buffering sinks that are not that type.

Why it matters. "Simple" is the stated goal. A new user must learn four types whose distinctions are subtle (sync vs async, value vs result, same-signal vs derived) before they can write a filter. The Collector answers the same question with one concept, `consumer.Traces` plus a `ProcessTracesFunc`, which is why its ecosystem of processors is large.

Recommendation. Collapse the matrix:

- Rename `pipeline.Processor<T>` to `AsyncTransform<T>` (or fold it into `Transform` as a single type with an async overload). The sync / async split is an optimization, not a user-facing concept.
- Rename the `processor/` package to something honest about its contents, for example `pipeline.ops` (built-in operations: batching, transforms, connectors), reserving `processor` for the future first-class component (F2).
- Rename `BatchingProcessor` to `Batcher` (it is a buffering sink). Reserve the `Processor` suffix across the whole codebase for the F2 component.
- Keep `Sink<T>` as the one terminal contract (it is the Collector `consumer` analogue and the right seam). Keep `Connector<I,O>` but generalize it (F5).

### F2 (P0). There is no first-class, extensible Processor component

What. The Collector's `processorhelper.NewTraces(cfg, next, func(ctx, td) (ptrace.Traces, error))` is the canonical, lowest-friction way to write a reusable processing stage: a function over one signal with a `next` consumer threaded in, plus lifecycle. otlp4j has no equivalent. The pieces exist but do not compose into a component:

- `Transform` and `Processor` are stateless functions, attached inline per pipeline (`Pipeline.from(s).transform(...)`), not reusable, named, lifecycle-bearing components.
- `BatchingProcessor` is the only stateful stage, and it is a terminal sink, not a mid-pipeline processor. It cannot be followed by another transform in the same chain without a second pipeline.
- `Connector` is the closest thing to a "component" (abstract class, lifecycle, `next` downstream), but it is signal-changing only and ships two hardcoded count connectors.

Evidence. There is no otlp4j type that is simultaneously (a) reusable and instance-able, (b) signal-typed, (c) lifecycle-bearing, and (d) threaded to a `next` consumer for same-signal processing. `Connector<I,O>` satisfies (a) and (c) but not (d) for same-signal I equals O without a user subclass. The README's "typed pipelines with transforms, filters, observers" sells a component story that the code delivers only as inline lambdas.

Why it matters. A gateway framework is, fundamentally, "receive, process, export" with the process step being the extension point users hit first. Today, to ship a reusable "drop all spans from noisy service X" processor, a user must either inline a `filter` in every pipeline or hand-roll a `Sink` that forwards to a captured downstream. Neither is a component model.

Recommendation. Introduce a first-class `Processor` (reusing the now-freed name from F1):

```java
// sketch: signal-typed, lifecycle-bearing, next-threaded
public interface TracesProcessor extends Lifecycle {
    CompletionStage<ConsumeResult> process(TracesData batch, TracesSink next);
}
// analogous MetricsProcessor / LogsProcessor / ProfilesProcessor
```

Provide a `processorhelper`-style base that owns the next-forwarding, the failure-to-`ConsumeResult` normalization, and the lifecycle cascade, so a user writes one method. Rebuild `BatchingProcessor` as a same-signal `TracesProcessor` (and so on for each signal) that forwards a merged batch to `next`, making it composable mid-chain. This is the single highest-leverage change for the "extensible foundation" goal.

### F3 (P0). Per-signal quadruplication has no generic spine

What. The four signals are enumerated about seven times over, with no `Signal`-keyed generic core to share the boilerplate:

- Model: `TracesData` / `MetricsData` / `LogsData` / `ProfilesData`, each with its own `Resource*` / `Scope*` nested records, `*Count()`, and `forEach*()`.
- API: four marker `@FunctionalInterface` sinks (`TracesSink` / `MetricsSink` / `LogsSink` / `ProfilesSink`); `OtlpExporter`'s four facets; `Receiver`'s four `Source`s; `TelemetryTap`'s four `Flow.Publisher`s; `Dispatchers`'s four functions.
- Codec: four mappers (`TraceMapper` / `MetricsMapper` / `LogsMapper` / `ProfilesMapper`) plus `SignalResponses`'s four near-identical switches.
- Transport-common: `SignalSource` instantiated four times; `ClientExporter`'s four `Facet` inner classes.
- gRPC: four service adapters; four `export*` methods.
- HTTP: four contexts; four `export*` methods; four URI fields.
- processor/internal: the `Signal` enum does key batch-type / merger / counter, proving the spine is possible, but it lives in a non-exported package and is used only by `BatchingProcessor`.

Evidence. `GrpcOtlpClient.exportTraces` / `exportMetrics` / `exportLogs` / `exportProfiles` are near-identical (they differ only in stub and mapper); `HttpOtlpClient` likewise (they differ in URI and mapper); `SignalResponses` has four switch arms that differ only in the `setRejected*(n)` builder call; `Signal` (in `processor.internal`) already abstracts the batch-type to merger to counter triple.

Why it matters. Adding a fifth signal, or writing "one processor that handles all signals," is O(signals) boilerplate today. The Collector centralizes this in `pdata` plus `consumer` per-signal interfaces; otlp4j re-derives the fan-out in every layer. This is the main drag on the codebase's size-to-surface ratio.

Recommendation.

- Promote `Signal` to an exported public type (for example `otlp4j.model.Signal` or `api.Signal`) carrying `{ batchType, merge(List<T>), itemCount(T), emptyBatch }`. Make `BatchMergers` its implementation detail.
- Add a generic `SignalBatch<T>` carrier (or at minimum a common `SealedBatch` interface) so codec, `Dispatchers`, `ClientExporter`, `SignalSource`, and the transport adapters can be written once against `Signal`-keyed generics rather than four times. The per-signal `*Sink` marker interfaces can remain (they are nice for type-safety at terminals), but they should be generated or trivially derived from the `Signal` enum rather than hand-maintained.
- Collapse the four `export*` / `dispatch*` / adapter methods to one generic `export(Signal, batch)` / `dispatch(Signal, batch)` using the promoted `Signal` key. The gRPC `GrpcServiceAdapters.dispatch` helper and the HTTP `handler(...)` factory already do this internally; generalize and lift it.

### F4 (P1). The shared batch timer is a JVM-global singleton that hurts isolation

What. `BatchingProcessor` nests a `SharedTimer` holding a JVM-wide static `ScheduledThreadPoolExecutor` ("one daemon thread drives every batcher's flush timer; never shut down"). It is the one genuinely global thread in the codebase: the transports' static `SHUTDOWN_EXECUTOR`s are plain `Executor` lambdas that spawn a named virtual thread per task and hold no state at rest, and `ClientExporter`'s static `Cleaner` is the standard leak-watch idiom; both are benign.

Evidence. `BatchingProcessor` Javadoc: "One JVM-wide platform daemon thread drives every batcher's flush timer. Never shut down; instances stay independent through per-instance timer cancellation." The thread starts on first batcher use and outlives every gateway instance.

Why it matters. A "robust foundation" gets embedded in apps, app servers, and tests, often several class-loaders or instances per JVM. The timer thread is a daemon, so it does not block JVM exit, but a static never-terminated thread pins its class loader for the life of the JVM (the classic redeploy leak in containers), and a misbehaving scheduled callback affects every batcher in the process. The Collector avoids this by giving each collector `service` its own telemetry and async machinery.

Recommendation.

- Make the batch timer owned, not static: a `BatchScheduler` (or reuse a `ScheduledExecutorService`) created per gateway / `Wiring` and shared by that gateway's batchers, closed on `Wiring.shutdown()`. Provide a default singleton only as a fallback, and document it.
- Keep the `Cleaner` and the per-task shutdown executors as they are; the leak-warning path should remain the only static state in `ClientExporter`.

### F5 (P1). `Connector` is single-downstream and ships only two built-ins; the signal-conversion matrix is not generalized

What. `Connector<I,O>` holds exactly one `downstream` sink; its `retain()` propagates to a `SharedLifecycle` downstream; failure semantics are governed by a `FailurePolicy` (`BEST_EFFORT` / `FAIL`); `Connectors` ships only `spanCount` and `logRecordCount` (both to metrics, subclasses of a sealed `CountConnector` base).

Evidence. `Connector` constructors take a single `downstream`; the Javadoc steers anything needing "asynchronous derivation or multi-batch emission" to implement `Sink` directly. The Collector's `connector` package defines a full N-by-M matrix (`TracesToMetrics`, `MetricsToLogs`, and so on) via a factory with per-pair creators, and its service graph hands each connector instance a router that fans out to every matching downstream pipeline (`connector.NewTracesRouter`).

Why it matters. Signal conversion (trace-to-metric counters, log-to-metric, metric-to-span sampling) is a core gateway need. With one downstream and two built-ins, every non-trivial connector is a from-scratch `Sink` implementation that must redo failure normalization, lifecycle, and refcounting by hand.

Recommendation.

- Generalize `Connector<I,O>` to fan out to multiple downstreams (or compose with `FanOut` internally), mirroring the Collector's framework-provided router.
- Promote the signal-conversion matrix to first-class: a `Connectors` registry or factory keyed by `(input Signal, output Signal)`, mirroring the Collector's `WithTracesToMetrics(...)` shape, so adding, say, `metricToSpan` or a second trace-to-metric is a factory call, not a subclass.
- Keep `derive(I) -> @Nullable O` as the single abstract method. It is the right granularity.

### F6 (P1). Lifecycle and ownership are correct but invisible and unvalidated

What. The ownership model is sophisticated: `SharedLifecycle` refcounting, structural `AutoCloseable` collection through `FanOut` peers, `Stage.owns(AutoCloseable)` for resources hidden behind lambdas, and `Wiring`'s three-phase (receivers, then pipelines, then owned) dedup-by-identity shutdown. It is all convention plus runtime best-effort. There is no ownership graph the framework checks.

Evidence. `PipelineLifecycle.leafResources` recurses through `FanOut` but cannot see a resource captured by a bare-lambda sink; the README explicitly tells users to call `Stage.owns(resource)` "for a resource hidden behind a lambda or a connector's downstream." A missed `owns()` silently leaks (the `ClientExporter` `Cleaner` only warns on the exporter specifically). `Wiring` dedups by identity across phases but does not detect cycles or dangling (never-attached) resources.

Why it matters. "Robust" means failures are loud. The most common lifecycle bug, forgetting `owns()` behind a lambda or double-wiring a shared exporter, produces a silent leak or a too-early close, both hard to diagnose. The Collector sidesteps this by building the graph itself (`service/internal/graph`) rather than asking the user to describe it.

Recommendation.

- Add a debug or validation mode (gated, cheap, off in production): a `Wiring.validate()` / `PipelineHandle.describe()` that walks the declared topology and reports (a) sinks that capture an `AutoCloseable` not declared via `owns`, (b) shared resources whose refcount will not balance, and (c) pipelines whose source is never attached. Even a partial heuristic here is a large robustness win.
- Consider whether `Wiring` should own the graph construction (a builder that takes receivers, processors, exporters and wires `next` consumers itself) rather than verifying a user-assembled graph. This is the path to eliminating the `owns()` escape hatch entirely (see F2).
- Make the `SharedLifecycle` retain / release symmetry explicit with a `release()` (currently release is implicit via `shutdown()`). Asymmetric verbs are a readability and audit tax.

### F7 (P1). Naming inconsistencies and dual enums

What. Several naming friction points beyond F1:

- Two `Overflow` enums: `processor.OverflowPolicy { DROP_OLDEST, DROP_NEWEST, BLOCK, FAIL }` (batcher) vs `receiver.TapOptions.Overflow { DROP_OLDEST, DROP_NEWEST, FAIL }` (tap, deliberately non-blocking). Same concept name, different types, slightly different option sets.
- Two failure-handling vocabularies for one concept: `Connector` takes a `FailurePolicy` enum (`BEST_EFFORT` / `FAIL`) while `BatchingProcessor` expresses the same downstream-failure question as a `reflectDownstreamFailures` boolean (default `false`).
- `GrpcRetry` holds only the pushback-trailer encoding; the actual retry logic lives in `ResilienceRetries` plus `GrpcOtlpClient`. The name implies more than it is.
- `Lifecycle.close()` default blocks (`shutdown(DEFAULT_GRACE_PERIOD).join()`), but `BatchingProcessor.close()` overrides to swallow `CompletionException` / `CancellationException` as WARN. The `close()` semantics are inconsistent between the default and the one override.
- `Wiring` is a single-owner `Lifecycle` (not `SharedLifecycle`), yet its constructor retains shared owned resources. It participates in refcounting without itself being refcountable.
- `OtlpClient extends AutoCloseable` (sync `close()`, no `Lifecycle`) while `OtlpServer extends Lifecycle` (async). Asymmetric SPI base contracts for the two transport halves (see F9).

Recommendation. Unify the two `Overflow` enums (one type, with `BLOCK` either removed or documented as batcher-only). Express downstream-failure handling in one vocabulary (`FailurePolicy`, or the `AckMode` of F8) instead of an enum on the connector and a boolean on the batcher. Rename `GrpcRetry` to `GrpcPushback`. Make `close()` semantics uniform (define a single swallow-vs-rethrow rule in `Lifecycle`). Align the SPI base contracts (F9).

### F8 (P1). `BatchingProcessor` acknowledges before delivery; the loss mode is neither named nor counted

What. `BatchingProcessor.consume(batch)` offers to the queue and returns `acceptedStage()` immediately. A timer or threshold drain that fails downstream is logged at WARN and the batches are lost with no upstream signal; only `reflectDownstreamFailures = true` (opt-in) reflects sustained failure back as retryable rejections. The public `droppedCount()` counts only overflow drops, so drain-failure losses are invisible even to a caller who polls it. The flush interval is a fixed one second (`FLUSH_INTERVAL`), not configurable.

Evidence. `BatchingProcessor.consume` does `queue.offer` then `acceptedStage()`; a failed flush in `BatchDrainEngine` logs at WARN and drops the drained batches without touching the `drops` counter. The Javadoc is honest about this ("enqueueing acknowledges immediately ... lost with no upstream signal").

Why it matters. This is the posture of the upstream defaults, not a divergence from them: the Collector's `batchprocessor` also acks on handoff and swallows send failures at WARN, and `exporterhelper`'s sending queue acks on enqueue unless `wait_for_result: true` is set, dropping and logging after retry exhaustion. But "the Collector is lossy by default too" is a weak defense for a gateway whose job is reliable forwarding. The difference is that the Collector names the choice (`wait_for_result`, `block_on_overflow`) and counts the losses (`otelcol_exporter_enqueue_failed_*`); here a user who does not read the warning gets at-least-once-on-ack semantics that are actually best-effort, with no counter that would reveal it.

Recommendation.

- Make the failure mode explicit and named (for example `AckMode.ON_ENQUEUE` vs `AckMode.ON_DELIVERY`, the split the Collector spells `wait_for_result`), with `ON_ENQUEUE` clearly documented as lossy and `ON_DELIVERY` the recommended default for forwarding pipelines. Keep `ON_ENQUEUE` for high-throughput tap-like use, but do not make lossy the silent default.
- Count drain-failure losses: `droppedCount()` should cover (or be joined by a counter that covers) batches lost to failed drains, not just queue overflow, so loss is observable rather than only logged.
- Make the flush interval a builder knob; a one-second constant is the kind of policy a gateway operator expects to tune (the Collector's batch `timeout` is configurable).

### F9 (P2). SPI asymmetry and a few hardening nits

What. Concrete hardening items, mostly small:

- `OtlpClient extends AutoCloseable` (sync `close()`), while `OtlpServer extends Lifecycle` (async). The client cannot expose `forceFlush` or bounded `shutdown` through the SPI; that richness lives only on the `ClientExporter` wrapper. Asymmetric.
- `maxConcurrentCallsPerConnection` is in shared `ServerConfig`, honored by gRPC (`NettyServerBuilder.maxConcurrentCallsPerConnection`), and silently ignored by HTTP (no builder knob, no warning, no doc on the HTTP builder). This is the one genuine cross-transport inconsistency.
- Double deadlines in both clients: gRPC applies `withDeadlineAfter` and also `orTimeout` on the future; HTTP applies `HttpRequest.timeout` and also `orTimeout`. Belt-and-suspenders and intentional, but a slow call can be interrupted twice; worth a comment if kept.
- `HttpExchangeHandlers.handle` blocks on `.join()` of the dispatcher stage, safe only because of the virtual-thread executor. If a user supplies a bounded `serverExecutor`, that pool becomes the sole concurrency cap (undocumented interaction with `AdmissionControl`).
- `shutdownNow()` is asymmetric across transports: the SPI method takes no timeout, gRPC waits a hard-coded 5s for termination, and HTTP stops immediately with `stop(0)`.
- `AdmissionControl.tryAcquire` can transiently overshoot `maxBytes` between concurrent admits (documented, rolled back). Acceptable, but worth noting.
- The qualified `.internal` export in `transport-common` lists only the two bundled transports, so a third-party transport cannot reuse `ClientHeaders` / `ResilienceRetries` without a module edit. Mild tension with the "extensible" goal.

Recommendation. Make `OtlpClient extends Lifecycle` too (or define a shared `TransportClient` lifecycle). Either honor `maxConcurrentCallsPerConnection` on HTTP (via the server executor or a per-connection semaphore) or remove it from shared `ServerConfig` and keep it gRPC-only (recommended: shared config carrying a transport-specific no-op is a leaky abstraction). Document the bounded-executor interaction. Drop the qualified export restriction for `ClientHeaders` / `ResilienceRetries`, or promote them to a small public `transport-common` helper API.

### F10 (P2). `FanOut` "parallel" claim is overstated; minor correctness and ergonomics

What. `FanOut.consume(batch)` launches peers sequentially in a loop on the caller thread; only the returned futures overlap. The "All peers run in parallel" documentation is true only if each peer's `consume` returns immediately without blocking work.

Evidence. `FanOut.consume` builds the `CompletableFuture[]` by calling each `peer.consume(batch)` in order before `allOf`.

Why it matters. A user wiring two HTTP exporters via `FanOut` expecting concurrent sends gets sequential dispatch (the sends then race, but the launch is serial). Minor, but the doc sets the wrong expectation.

Recommendation. Either (a) launch each peer's `consume` on a per-peer virtual thread (cheap, makes "parallel" true), or (b) fix the doc to say "dispatched sequentially, completed concurrently." Prefer (a) for a gateway where fan-out-to-N-exporters is a common latency-sensitive path. Also: the `fanOutMerge` `Partial` aggregation uses `max` rejected-items (correct, since all peers see the same batch); keep it, but document the rationale inline.

### F11 (P2). Observability of the framework itself is absent

What. There is no built-in way to observe the gateway's own health as a whole: queue depths, drop counts, drain failures, per-signal throughput, or end-to-end latency. The raw counters are scattered and half-reachable: `BatchingProcessor.droppedCount()` / `queued()` and `TelemetryTap.droppedCount()` are public but ad-hoc getters, `AdmissionControl` exposes `rejectedCount()` / `inFlightRequests()` / `inFlightBytes()` but the instance is created inside `server.start()` and is unreachable from user code, drain-failure losses are counted nowhere (F8), and the exporters keep no success or failure counts at all.

Why it matters. A "robust" gateway must be operable. The Collector ships `component.TelemetrySettings` so every component emits its own metrics via the host, and even the Java SDK's `BatchSpanProcessor` self-instruments its queue (size, dropped, processed). otlp4j has part of the raw signal but no plumbing.

Recommendation. Add a lightweight `GatewayMetrics` / `TelemetryRegistry` SPI that `BatchingProcessor`, `AdmissionControl`, `ReceiverTap`, and `ClientExporter` push counters to, with a no-op default and a logging / micrometer / otel-SDK adapter as samples. This is also the natural place to self-instrument the gateway with the OpenTelemetry Java SDK (positioned correctly per the README's "use otel-java for instrumentation").

### F12 (P2). OTLP surface parity: no OTLP/JSON, partial env-var support

What. The HTTP transport speaks binary protobuf only (`application/x-protobuf`); there is no OTLP/JSON encoding on either the receiver or the exporter, and the receiver does not inspect `Content-Type`, so a JSON request is answered with `400 malformed protobuf` rather than a content-negotiation error. `OtlpEnv` reads only the general client variables (`OTEL_EXPORTER_OTLP_ENDPOINT` / `HEADERS` / `TIMEOUT` / `COMPRESSION` / certificates / `INSECURE`); the per-signal forms (`OTEL_EXPORTER_OTLP_TRACES_*` and siblings) and `OTEL_EXPORTER_OTLP_PROTOCOL` are not recognized, and there is no server-side env config.

Why it matters. A gateway receives from clients it does not control. OTLP/JSON is the encoding a browser, a curl probe, or a lightweight producer is most likely to send, and the Collector's OTLP receiver accepts it; here it fails with a misleading error. The env-var gaps are smaller but confound users arriving from otel-java or the Collector, where those variables work.

Recommendation. Either add an OTLP/JSON codec path on the HTTP receiver (the exporter can stay protobuf-only), or state protobuf-only as an explicit scope decision in the README and reject unsupported content types with a clear 415. Extend `OtlpEnv` with the per-signal variables, or document precisely which subset of the spec it implements.

---

## 5. Summary scorecard

| Dimension | Rating | Notes |
| --- | --- | --- |
| Module boundaries / proto isolation | 5/5 | Textbook JPMS usage; proto never leaks. |
| Domain model (immutability, fidelity) | 5/5 | Records, construction-time validation, honest profiles / NoData handling. Better than Collector's mutable pdata. |
| Core contract (`ConsumeResult` plus async `Sink`) | 5/5 | End-to-end, retry-aware, consistently encoded per transport. |
| Transport abstraction and shared tier | 4/5 | Genuinely shared; per-signal boilerplate, the `maxConcurrent` inconsistency, and the protobuf-only HTTP surface (F12) detract. |
| Receiver hardening (admission, tap) | 4/5 | Bounded admission the core Collector lacks; tap outside the ack path is well-designed. |
| Vocabulary / API simplicity | 2/5 | Overloaded terms, four fragmented SAM types (F1). |
| Extensibility (Processor component) | 2/5 | No first-class processor; BatchingProcessor misnamed (F2). |
| De-duplication / generic spine | 2/5 | Per-signal quadruplication everywhere (F3). |
| Lifecycle robustness | 3/5 | Correct but invisible and unvalidated; the lossy batching default matches upstream defaults but is unnamed and uncounted (F6, F8). |
| Operability / self-observability | 2/5 | Counters are scattered and half-reachable; no registry (F11). |

---

## 6. Recommended phased path

Ordered so each phase is independently shippable and the highest-leverage changes come first.

Phase 0, Vocabulary (P0, low risk, high clarity). F1. Rename `pipeline.Processor` to `AsyncTransform` (or fold into `Transform`); rename the `processor/` package (for example `pipeline.ops`); rename `BatchingProcessor` to `Batcher`. Free the `Processor` name. Pure renames plus doc updates; no behavior change.

Phase 1, Generic signal spine (P0, medium risk). F3. Promote `Signal` to an exported type with `{ batchType, merge, itemCount, empty }`; introduce a `SignalBatch<T>` carrier; collapse the four-fold `export*` / `dispatch*` / adapter methods to `Signal`-keyed generics. This touches codec, transport-common, and both transports, but each is a mechanical de-duplication. It pays for itself immediately in maintainability and is a prerequisite for F2 and F5.

Phase 2, First-class Processor component (P0, the keystone). F2. Introduce signal-typed `TracesProcessor` and so on, extending `Lifecycle` with a `process(batch, next)` contract; provide a `processorhelper` base owning next-forwarding, failure normalization, and the lifecycle cascade. Rebuild `Batcher` as a same-signal `Processor` that forwards a merged batch to `next` (composable mid-chain). This is what turns otlp4j from "a pipeline DSL" into "an extensible gateway framework."

Phase 3, Robustness defaults (P1). F8 (name the ack modes and default forwarding pipelines to delivery-ack, count drain-failure losses, make the flush interval configurable), F4 (owned batch scheduler instead of the JVM-global timer), F6 (debug / validation mode for the ownership graph, explicit `release()`). These make "robust" true by default rather than by careful reading.

Phase 4, Extensibility breadth (P1). F5 (generalize `Connector` to multi-downstream plus an `(in, out)`-keyed `Connectors` matrix), F11 (self-observability registry plus an otel-SDK adapter sample), F7 / F9 / F10 (enum / naming / SPI cleanup, the `maxConcurrentCallsPerConnection` resolution, real `FanOut` parallelism), F12 (OTLP/JSON on the HTTP receiver or an explicit protobuf-only stance, env-var parity).

Non-goals worth stating. Do not adopt the Collector's mutable-`pdata` model: otlp4j's immutability is a structural advantage (no `MutatesData` / clone-on-fanout machinery). Do not adopt the Collector's config-graph (`confmap` / component factories) yet: otlp4j's code-as-configuration builder DSL is the right shape for an embedded JVM library, and a graph / config layer can come later if a standalone distribution is ever wanted. Keep the two-transport, no-provider-discovery model: instantiating a concrete `OtlpHttpReceiver` is simpler and more Java-idiomatic than SPI discovery.

---

## 7. Verdict

otlp4j is a disciplined, well-layered foundation with two genuinely superior design choices (immutable records, async `ConsumeResult`) and a clean transport SPI. To become a simple yet robust, extensible OpenTelemetry gateway foundation, the work is concentrated, not scattered: collapse the vocabulary (F1), introduce a first-class Processor component (F2), and de-duplicate the per-signal spine (F3). Doing those three moves the codebase from "a good OTLP library with a pipeline DSL" to "the JVM's extensible Collector equivalent," with structurally cleaner data-handling than its Go namesake.
