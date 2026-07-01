# otlp4j Design & Structure Review: Toward an Extensible OTLP Gateway

_Targeted architecture and API review. Scope: is otlp4j a simple, robust foundation for an extensible OpenTelemetry gateway in the mould of the OpenTelemetry Collector? Where should the design be simplified, and where should the implementation be hardened?_

Reference points used for comparison: the OpenTelemetry **Collector** (`consumer` / `component` / `service/internal/graph` / `connector` / `exporterhelper` / `config*`) and the OpenTelemetry **Java SDK** exporter/config abstractions. Breaking changes are assumed to be on the table, module, package, and type renames included.

---

## 1. Verdict

otlp4j is a genuinely well-built library. The module graph is strict and one-way, the domain model is fully immutable, proto/gRPC types are quarantined behind two interchangeable transports, and the engineering hygiene is high (JDK 25, `failOnWarning` + `-Xdoclint`, 509 tests, 90% line / 80% branch JaCoCo gates). Two of its core choices are, in my view, _cleaner than the Collector's_:

- **Immutable records replace the entire `MutatesData`/clone machinery.** The Collector's single most important pipeline optimization, a per-component `Capabilities{MutatesData}` bit plus a fan-out that clones once per mutating branch and shares among readers, is simply unnecessary here. Immutable `TracesData`/`MetricsData`/... let every fan-out peer share one reference with zero copies and zero capability bookkeeping. This is a real simplification win.
- **Async `CompletionStage<ConsumeResult>` with a sealed `Accepted`/`Partial`/`Rejected` result** is a more expressive consumer contract than the Collector's synchronous `Consume(ctx,data) error`.

The gap between what otlp4j _is_ and the "extensible gateway like the Collector" framing is narrower than it first looks, but it is real, and it clusters in four places:

1. **The extension surface is thinner than the framing implies.** There is exactly one general processing primitive (`Transform<T>`, synchronous 1-to-1), one buffering primitive (`BatchingProcessor`), and two hardcoded connectors (span-count, log-record-count). There is no general `Processor` SPI and no general signal-changing `Connector`. Anything else means implementing `Sink` + `Lifecycle` by hand.
2. **The lifecycle/ownership model is the most intricate part of the whole API, and it exists to compensate for the absence of a graph.** `Stage.owns(...)`, `SharedLifecycle` reference counting, `Cleaner`-based leak detection, and the "auto-collect `AutoCloseable` peers" rules are all hand-rolled substitutes for what the Collector's `service/internal/graph` derives automatically (node dedup, topological start/stop, fan-out insertion).
3. **Resilience (retry, timeout, batching) is duplicated and _divergent_ across the two transports** instead of shared. The same `RetryPolicy` + `timeout` produce materially different worst-case behaviour on gRPC vs HTTP. The Collector consolidates all of this into one `exporterhelper` sender chain; otlp4j re-implements it twice.
4. **A handful of naming/surface issues** blunt the "simple" goal, most notably a _two-SPI naming collision_ and an _undocumented public module_.

None of these are structural dead-ends. The foundation is sound; the recommendations below are about consolidation and a few targeted extension points, not a rewrite.

**Priority snapshot** (detail in sections 6 and 7):

| # | Theme | Recommendation | Type | Effort |
| --- | --- | --- | --- | --- |
| P1 | Resilience | Unify retry/timeout into one shared implementation; fix HTTP global-deadline plus add jitter/`Retry-After` | Harden | M |
| P2 | Surface | Resolve the two-SPI collision; document (or fold) `otlp4j-transport-spi` | Simplify | S |
| P3 | Extensibility | Add a general `Connector<I,O>` and decide `Transform` async story; keep `Transform` as the sync sugar | Design | M |
| P4 | Lifecycle | Treat the ownership model as a symptom: either a minimal graph, or shrink `owns` surface | Design | M/L |
| P5 | Robustness | Global admission control (in-flight bytes/count limiter) for the receiver | Harden | M |
| P6 | Boilerplate | Remove the ~4x builder-delegation duplication in the transport entry points | Simplify | S |

---

## 2. What the codebase is (inventory)

~9,500 lines of main source across 8 JPMS modules (plus generated proto):

| Module | Role | Notes |
| --- | --- | --- |
| `otlp4j-model` | Immutable OTLP domain records (17 public types) | JDK-only, no proto |
| `otlp4j-api` | Pipeline DSL, processors, config, receiver, `spi` (28 public types across 5 packages) | The user-facing core |
| `otlp4j-transport-spi` | **Shared transport impls** (`ClientExporter`, `ServerReceiver`, `SignalSource`, tap) | _Undocumented_; publicly exported |
| `otlp4j-codec` | model/proto marshalling plus result mapping | qualified-exported to transports |
| `otlp4j-proto` | Generated OTLP messages/services | qualified-exported |
| `otlp4j-transport-grpc` | OTLP/gRPC (gRPC + Netty) | ~900 LOC |
| `otlp4j-transport-http` | OTLP/HTTP (JDK only) | ~1,040 LOC |
| `otlp4j-samples` / `-testing` / `-coverage` | E2E demo, fixtures, aggregate coverage |  |

Build posture is a strength and should be preserved as-is: `maven.compiler.release=25`, `failOnWarning=true`, `-Xdoclint:all,-missing`, Javadoc `failOnWarnings=true`, JaCoCo 0.90/0.80.

---

## 3. Architecture evaluation

### 3.1 Module boundaries: excellent, keep

The dependency graph is strictly one-way (`transport` to `api` to `model`, `transport` to `codec` to `proto`), enforced by `module-info` qualified exports rather than convention. `otlp4j-proto` exports its generated packages _only_ to the codec and the two transports; `api` re-exports `model` transitively so signatures stay proto-free. An HTTP-only deployment never pulls in gRPC/Netty. This is the part of the design that most resembles a mature framework and needs no change.

### 3.2 The consumer contract vs the Collector

|  | Collector | otlp4j |
| --- | --- | --- |
| Contract | `ConsumeTraces(ctx, td) error` (sync push) | `Sink<T>.consume(T)` returns `CompletionStage<ConsumeResult>` (async) |
| Result granularity | `error` or `nil` (all-or-nothing) | `Accepted` / `Partial(n,msg)` / `Rejected(retryable,cause)` |
| Partial success | wire-edge only (logged on export; built at ingress) | **in-band**, threaded through the whole pipeline |
| Cancellation | `context.Context` per call | none (no per-call context) |
| Buffer ownership | callee must not retain; may mutate if declared | records immutable, retain/share freely |

otlp4j's contract is the better _abstraction_: a `Sink` is uniform across receiver source, transform terminal, exporter facet, batcher, and connector, exactly like the Collector's "everything is a consumer," but it carries richer results and never needs a mutation capability.

Two consequences of threading `Partial` in-band deserve a design note:

- **Semantic drift through transforms.** When a pipeline transforms/filters/enriches before the terminal, the terminal's `Partial(n)` count is expressed in _downstream_ item identity, but it is reported back to the _upstream_ sender as its partial-success count. After a filter or a count-changing transform, "3 rejected" no longer maps to 3 of the items the sender actually sent. The Collector sidesteps this by keeping partial-success strictly at the edges. This is not a bug, but it is a subtle correctness caveat worth documenting (and worth bounding: a gateway arguably should not forward a downstream per-item `Partial` upstream verbatim once the batch has been reshaped).
- **No cancellation.** For a long-lived gateway, the absence of any per-call cancellation/deadline token means a slow synchronous `Transform` on the transport dispatch thread has no cooperative cancellation. Mitigated today by virtual-thread executors, but see 3.5.

### 3.3 The pipeline model vs the Collector graph: the central gap

otlp4j's `Pipeline.from(source).transform(...).filter(...).to(terminal)` is a **per-signal linear chain** with explicit `FanOut.of(...)` for branching and manual `owns(...)` for lifecycle. It is pleasant for the 1-receiver-to-N-exporter case and is the right default for "simple."

What it is _not_ is a graph, and the framing ("foundation of an extensible gateway") invites the comparison. The Collector's `service/internal/graph`:

- dedups shared component instances (one receiver feeding two pipelines is one node);
- inserts fan-out automatically and computes aggregate mutation once;
- **derives** startup order (downstream-first) and shutdown order (upstream-first) topologically;
- detects cycles (which only connectors can introduce) with a readable error.

otlp4j reproduces slices of this _by hand and by convention_: the user must order shutdown themselves ("receiver first, then subscription", documented, not enforced), must wrap multi-consumer attach in `FanOut` (a `Source` has a single attachment slot and throws on the second `subscribe`), and must reason about which resources are auto-drained vs need `owns(...)`. The ownership model (3.6) is the price of not having a graph.

**Recommendation is a fork, not a foregone conclusion** (see 6-P4): either (a) keep the linear DSL as the simple front door and _reduce_ the ownership surface, or (b) introduce a minimal internal graph that subsumes `owns`, fan-out, and reference counting. Do not add a heavyweight factory/registry graph like the Collector's; that would forfeit the simplicity that is otlp4j's main advantage.

### 3.4 Error / acknowledgement model: strong

The `ConsumeResult` to wire mapping (`codec/SignalResponses`, `codec/DeliveryResults`) is clean and correct: `Accepted` leaves `partial_success` unset, `Partial` carries the rejected count, a whole-batch `Rejected` is _never_ encoded as `rejected=0` (which would read as success) but mapped to a transport error, retryable to gRPC `UNAVAILABLE` / HTTP `503`, permanent to `INTERNAL` / `500`. This matches the Collector's `consumererror` two-axis model (permanent vs retryable plus status translation).

Two things the Collector has that otlp4j lacks:

- **Server-requested backoff** (`NewThrottleRetry(err, delay)` / honoring `Retry-After`). otlp4j's `Rejected` cannot carry a retry-after hint, and neither client honors one. See 6-H2.
- **A throttle/`Downstream` distinction** for instrumentation. Minor; not needed for a first cut.

### 3.5 Processing is synchronous-only

`Transform<T>` is `T apply(T)`, synchronous, 1-to-1, run on the transport dispatch path. There is no async processing stage (for example a transform that awaits an enrichment lookup) short of implementing an observing `Sink` or a fan-out peer. For a gateway that may do I/O-bound enrichment (attribute lookup, sampling decisions against an external service), the only in-band option today blocks a dispatch thread. This is a deliberate simplicity choice and is fine for the stateless built-ins, but the "extensible" story should make the async path a first-class, documented option rather than an implicit "implement `Sink` yourself."

### 3.6 Lifecycle & ownership: correct, but the most complex surface

The lifecycle code is careful and correct: single shared deadline across a drain, best-effort teardown that continues past the first failure, `SharedLifecycle.retain()` reference counting so a multi-signal exporter shared by two subscriptions closes only on the last release, and `Cleaner`-based leak warnings when an exporter is GC'd without shutdown. The `BatchDrainEngine` (coalescing, serial, mutex-guarded tail chaining) is a highlight.

But the _conceptual_ surface a user must hold is large:

- terminals and fan-out peers that implement `AutoCloseable` are auto-drained;
- exporter facets carry the whole exporter's lifecycle;
- count connectors cascade to their downstream;
- a `BatchingProcessor` drains its own queue but **not** its downstream, so you must `owns(...)` the exporter behind it;
- a bare-lambda terminal hides its exporter, so you must `owns(...)` that too;
- shutdown order (receiver, then subscription) is the caller's responsibility.

That is a lot of rules for "close everything once." Every one exists because the framework cannot _see_ the graph. This is the strongest argument that the ownership model and the missing graph are the same problem viewed from two sides (6-P4).

### 3.7 Resilience is re-implemented per transport, and diverges

This is the most consequential _implementation_ finding.

- **gRPC** maps `RetryPolicy` onto gRPC's native retry (service config) and lets the per-request **deadline bound total wall time**; backoff is inside the deadline.
- **HTTP** (`HttpOtlpClient.export`) runs an explicit attempt loop where each attempt gets the full `config.timeout()` **and** `Thread.sleep(backoff)` is added _on top between_ attempts. There is no global deadline budget across attempts.

So `setTimeout(10s)` plus default `RetryPolicy` (5 attempts, 1s to 5s) means, worst case, ~10s on gRPC but potentially `5 x 10s + (1+1.5+2.25+3.4)s`, about 58s, on HTTP for the _same config_. A caller cannot reason about a gateway's upstream latency without knowing which transport is underneath. Neither transport applies **jitter** (the Collector's `configretry` has `RandomizationFactor`), so many senders retrying a recovering collector synchronize into a thundering herd. Neither honors a server-sent `Retry-After` / throttle.

The Collector's answer is instructive: `exporterhelper` composes `timeout` to `retry` to `queue` to `batch` as decorators over one narrow `Sender.Send(ctx, req) error`, so every transport inherits identical semantics. otlp4j should share at least the retry/timeout logic (see 6-P1).

### 3.8 Batching model: the older Collector shape

`BatchingProcessor` is a pipeline **stage** (attach as terminal or fan-out peer). That mirrors the Collector's legacy `batchprocessor`, not its current direction (batching has moved _into_ `exporterhelper`'s sending queue, on the async dequeue path, shared with retry/persistence). Two implications:

- **Immediate-ack semantics.** `BatchingProcessor.consume` enqueues and returns `Accepted` synchronously; a later downstream `Rejected`/`Partial` on drain is invisible to the original sender (surfaced only via `BatchDeliveryException` from `forceFlush`/`shutdown` and logs). This is the standard async-batching trade-off, but it means placing a batcher in a pipeline silently converts end-to-end acknowledgement into best-effort. Worth stating loudly in the API docs.
- **No persistent queue.** The Collector offers a storage-backed queue for crash durability. Out of scope for a first cut, but the batching layer is where it would live, which is a second reason to consider consolidating batching toward the exporter edge over time.

---

## 4. Comparison tables

### 4.1 otlp4j vs OpenTelemetry Collector

| Concern | Collector | otlp4j | Assessment |
| --- | --- | --- | --- |
| Consumer contract | `Consume(ctx,data) error` | `Sink` returns `CompletionStage<ConsumeResult>` | otlp4j richer |
| Mutation safety | `MutatesData` plus clone-once fan-out | immutable records | **otlp4j simpler** |
| Partial success | wire edges only | in-band through pipeline | otlp4j expressive; watch semantic drift (3.2) |
| Error axes | permanent/retryable plus throttle plus status | permanent/retryable plus status | otlp4j lacks throttle/`Retry-After` |
| Wiring | declarative graph, topo start/stop, dedup, cycle check | linear DSL plus explicit `FanOut` plus manual `owns` | **Collector stronger; the core gap** |
| Connectors | 3x3 signal matrix, general | 2 hardcoded counts | **otlp4j much narrower** |
| Resilience | one `exporterhelper` chain, all transports | duplicated per transport, divergent | **otlp4j weaker (P1)** |
| Batching | in exporter queue (current) | pipeline stage (legacy shape) | acceptable; note trade-offs |
| Config factoring | shared `configgrpc/http/tls/retry`, `Optional[T]` | shared `ClientConfig/ServerConfig/Tls/RetryPolicy` | **good parity** |
| Admission control | `memory_limiter` plus queue bounds | per-request size cap plus per-conn concurrency | **no global limiter (P5)** |

### 4.2 otlp4j vs OpenTelemetry Java SDK

| Concern | otel-java SDK | otlp4j | Assessment |
| --- | --- | --- | --- |
| Async result type | `CompletableResultCode` (narrowed, JDK-8 era) | `CompletionStage<ConsumeResult>` | **otlp4j more modern** |
| gRPC vs HTTP | separate classes; runtime pick in autoconfigure | separate classes; pick by class | parity |
| Builder setters | uniform `set*` | `set*` for config/transport, no-prefix for model/pipeline builders | deliberate split; mild inconsistency |
| `RetryPolicy` | AutoValue; `maxAttempts` capped at 5; has exception predicate | record; `maxAttempts` uncapped; no predicate | defaults match; otlp4j slightly looser |
| Fan-out/composite | `composite()` collapses empty to noop, single to identity | `FanOut.of` requires >=1, always wraps | minor optimization opportunity |
| Env precedence | sysprops > env > default; per-signal overrides; normalized keys | env = lowest only; no sysprops; no per-signal | deliberate determinism; feature gap |
| Nullness | JSR-305 `@ParametersAreNonnullByDefault` | JSpecify `@NullMarked` | **otlp4j more modern** |
| Immutability | AutoValue (Java 8) | records | **otlp4j more modern** |

---

## 5. What's genuinely good (keep, don't touch)

- Strict JPMS module graph and proto quarantine (3.1).
- Fully immutable model with copy-modify helpers (`toBuilder`, `Attributes.with`, `of(...)` factories), `Metric.NoData` instead of null, defensive `byte[]` cloning, construction-time ID/flag validation in `Ids`.
- The `Sink` / `ConsumeResult` core and its wire mapping (`SignalResponses`/`DeliveryResults`).
- `BatchDrainEngine` concurrency design; `ClientExporter` reference counting plus `Cleaner` leak watch.
- `@NullMarked` everywhere with a small, documented set of `@Nullable` spots.
- Build/test hygiene (warnings-as-errors, doclint, coverage gates, 509 tests).

---

## 6. Findings & recommendations

Severity: **[H]** hardening/robustness, **[S]** simplification, **[D]** design/extensibility, **[N]** naming/docs. Effort: S/M/L.

### A. Structural & naming

**A1 [N][S] Two "SPI" concepts collide.** `dev.nthings.otlp4j.spi` (in `api`: `OtlpClient`, `OtlpServer`, `Dispatchers`, the _implement a new wire transport_ SPI) and the separate `otlp4j-transport-spi` module / `dev.nthings.otlp4j.transport.spi` package (`ClientExporter`, `ServerReceiver`, `SignalSource`, `MulticastPublisher`, `ReceiverTap`, shared _implementations_, not a service-provider interface). A transport author must use both, and the near-identical names invite confusion. **Action:** rename the shared-impl module to what it is, a transport _kit/support_ layer (for example `otlp4j-transport-kit`, package `...transport.support`), or fold it into the two transports if it has no third consumer. Reserve "spi" for the actual contracts. Effort: S.

**A2 [N] `otlp4j-transport-spi` is undocumented.** It is absent from the README module table, `architecture.md`, and `public-api.md`, yet it is a publicly exported module every transport depends on. Either document it as a first-class extension point or make it non-public. Effort: S.

**A3 [S] Builder-delegation duplication.** `OtlpGrpcExporter.Builder` and `OtlpHttpExporter.Builder` each re-declare ~15 setters that do nothing but delegate to `ClientConfig.Builder`; the two receiver builders do the same over `ServerConfig.Builder`. That is four near-identical wrappers. The _reason_ is real (per-transport Javadoc; HTTP-only `setPath`/`setConnectTimeout` hidden from gRPC), so do not just delete them blindly. **Action:** factor a shared abstract/base transport builder, or expose `ClientConfig`/`ServerConfig` as the primary configuration door with the entry points as thin `from(config)` factories plus only the transport-specific knobs. Effort: S/M.

**A4 [N] Cosmetic inconsistencies.** Column-aligned whitespace in `OtlpClient`/`Dispatchers` signatures; `Pipeline.Stage.transform` javadoc ("a transform that returns null rejects the batch") contradicts `Transform.apply`'s contract ("returns a non-null batch"), in practice a null return throws NPE that is caught and remapped to a permanent rejection, that is control-flow-by-exception. Pick one contract and state it. Effort: S.

### B. API design & extensibility

**B1 [D] Add a general `Connector<I,O>`.** Today `Connectors` ships exactly two hardcoded trace/log-to-metric counters. The Collector's connector is a general signal-changing bridge (the count connector is one instance of it). A minimal `interface Connector<I,O>` (a `Sink<I>` that emits `O` to a downstream `Sink<? super O>`, carrying lifecycle cascade) would let users build routing, span-metrics, and exceptions connectors without dropping to raw `Sink`+`Lifecycle`. Keep the two built-ins as instances. Effort: M.

**B2 [D] Decide the async-processing story.** Make an async/`CompletionStage` processing stage a first-class, documented option (a general `Processor<T>` SPI, or an `asyncTransform`), with `Transform<T>` remaining the synchronous sugar for the stateless common case. Right now "do I/O in a stage" has no blessed answer. Effort: M.

**B3 [D] `RetryPolicy` parity.** Add optional **jitter** (Collector `RandomizationFactor`), and consider matching otel-java's `maxAttempts` guardrail and a retryable-exception predicate. See also H1/H2. Effort: S (as part of P1).

**B4 [S] `FanOut` collapse.** Match otel-java `composite`: a single-peer `FanOut` can return the peer directly and an empty one can be a documented no-op sink, avoiding a wrapper on the hot path. Minor. Effort: S.

**B5 [D] Single-slot `Source`.** The one-attachment-slot rule (second `subscribe` throws) forces all branching into a single `FanOut` graph and is a corollary of "no graph." Fine to keep for simplicity, but call it out explicitly in `Source`'s contract and cross-link `FanOut`; today the constraint is discovered by exception. Effort: S.

### C. Hardening

**C1 [H] Unify retry/timeout across transports (P1).** Extract one retry/backoff/deadline implementation (a shared "sender" concern in `codec` or `transport-spi`) and drive both transports through it, or at minimum make the two honor the _same_ contract: a **global deadline budget across attempts** (fix the HTTP per-attempt-timeout plus stacked-sleep behaviour in `HttpOtlpClient.export`), the same retryable-condition set, jitter, and `Retry-After`/throttle honoring. Document the wall-time guarantee precisely. Effort: M. _This is the highest-value hardening item._

**C2 [H] Honor server backoff.** Parse `Retry-After` (HTTP 429/503) and gRPC throttling into the backoff, and let a `Rejected` optionally carry a retry-after hint so a gateway can propagate downstream backpressure upstream. Effort: S/M.

**C3 [H] Global admission control (P5).** The receiver has a per-request size cap and optional per-connection concurrency, but no _global_ in-flight bound (bytes or count). A gateway under load can accumulate many concurrent in-flight batches and OOM within the per-request limits. Add a Collector-`memory_limiter`-style global limiter (bounded in-flight bytes/count that sheds with a retryable `Rejected`). Effort: M.

**C4 [H] Batching backpressure clarity.** Document prominently that a `BatchingProcessor` in a pipeline converts synchronous end-to-end acknowledgement into best-effort (immediate `Accepted` on enqueue; downstream failures surface only via drain exceptions/logs). Consider a mode that reflects sustained downstream failure back as retryable at ingress once the queue is saturated. Effort: S (docs) / M (mode).

**C5 [H] Header validation.** `Transports.resolveHeaders` merges a `Supplier`-provided map without validating null values or transport-illegal characters (gRPC metadata / HTTP header constraints). Validate at build/attach. Effort: S.

**C6 [H] Tap `BLOCK` policy.** `OverflowPolicy.BLOCK` on a `TelemetryTap` back-pressures the receiver's publish thread, that is, a slow _observer_ can stall the in-pipeline path, defeating the tap's "outside the ack path" guarantee. It is documented as "risky"; consider removing `BLOCK` for taps (keep it for the batcher) so the type system cannot express the footgun. Effort: S.

### D. Model fidelity

**D1 [D] Hex-string trace/span IDs.** IDs are validated hex `String`s (`Ids`). Ergonomic and debuggable, but on a high-throughput gateway every ID is a heap `String` and every codec crossing parses/formats hex. If profiling shows ID handling on the hot path, offer a `byte[]`/`long`-pair representation behind the same accessors. Measure first; do not pre-optimize. Effort: M.

**D2 [N] Profiles remain `v1development`.** Correctly `@Experimental` with lossless opaque passthrough. No action beyond tracking the stable schema; keep the "forward 1:1, don't batch across distinct dictionaries" guidance loud. Effort: none.

---

## 7. A target shape for "simple, yet extensible"

If the goal is a Collector-class _foundation_ without the Collector's weight, the minimal set of first-class extension points is:

1. **Wire transport SPI**, `OtlpClient` / `OtlpServer` / `Dispatchers` (already good; just fix the naming in A1/A2).
2. **`Processor<T>`**, the async, stateful processing contract; `Transform<T>` stays as the sync sugar (B2).
3. **`Connector<I,O>`**, the general signal-changing bridge; the two counts become instances (B1).
4. **`Exporter` / `Sink` plus `Lifecycle`**, already the terminal contract.
5. **One shared resilience layer**, retry/timeout/queue/batch applied uniformly (C1), the way `exporterhelper` is one thing rather than per-transport code.

And, explicitly, what to **keep out** to stay simple (these are where otlp4j is _right_ to diverge from the Collector):

- No `ServiceLoader`/factory/registry indirection, instantiate transports by class (keep).
- No `MutatesData` capability, immutability makes it moot (keep).
- No YAML-config/service-graph engine, the fluent DSL is the front door. _If_ a graph is added (P4), it should be an internal wiring detail that the DSL compiles to, not a user-facing config language.

The single most clarifying reframe: **the ownership model (`owns`, `SharedLifecycle`, leak detection) and the missing graph are the same problem.** Whichever direction P4 goes, shrink the ownership surface or introduce a minimal internal graph, decide it deliberately, because it is the line between "simple linear pipeline library" and "extensible gateway foundation," and today the code straddles it.

---

## 8. Suggested sequencing

- **Now (quick wins, low risk):** A1 through A4 (naming/docs/dedup), B4, C5, C6, C4-docs.
- **Next (the resilience consolidation):** C1 plus C2 plus B3, one shared, jittered, deadline-honoring retry path across both transports. Highest robustness ROI.
- **Then (extensibility):** B1 (`Connector<I,O>`), B2 (`Processor<T>`/async), C3 (admission control).
- **Deliberate, larger:** P4 (ownership-vs-graph decision), D1 (ID representation, only if measured).

The foundation is strong enough that none of this is rework, it is consolidation of duplicated resilience code, a couple of named extension points, and one architectural decision about wiring.
