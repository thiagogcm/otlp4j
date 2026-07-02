# otlp4j Design & Structure Review: Toward an Extensible OTLP Gateway

_Targeted architecture and API review. Scope: is otlp4j a simple, robust foundation for an extensible OpenTelemetry gateway in the mould of the OpenTelemetry Collector? Where should the design be simplified, and where should the implementation be hardened?_

Reference points: OpenTelemetry **Collector** (`consumer`, `component`, `service/internal/graph`, `connector`, `exporterhelper`, `config*`) and OpenTelemetry **Java SDK** exporter/config abstractions. Breaking changes are assumed to be on the table.

---

## 1. Verdict

otlp4j is a genuinely well-built library. The module graph is strict and one-way, the domain model is fully immutable, proto/gRPC types are quarantined behind two interchangeable transports, and engineering hygiene is high (JDK 25, `failOnWarning` + `-Xdoclint`, 606 tests, 90% line / 80% branch JaCoCo gates). Two core choices are _cleaner than the Collector's_:

- **Immutable records** eliminate the Collector's `MutatesData`/clone machinery. Every fan-out peer shares one reference with zero copies and zero capability bookkeeping.
- **Async `CompletionStage<ConsumeResult>`** with sealed `Accepted`/`Partial`/`Rejected` is a more expressive consumer contract than the Collector's synchronous `Consume(ctx,data) error`.

The gap to an "extensible gateway like the Collector" clusters in three places:

1. **Thin extension surface** — one general processing primitive (`Transform<T>`, sync 1-to-1), one buffering primitive (`BatchingProcessor`), two hardcoded connectors (span-count, log-record-count). No general `Processor` SPI or signal-changing `Connector`.
2. **Lifecycle complexity** — `Stage.owns(...)`, `SharedLifecycle` reference counting, `Cleaner`-based leak detection, and auto-collect rules compensate for the absence of a wiring graph.
3. **Naming/surface issues** — a two-SPI naming collision and an undocumented public module.

The foundation is sound. Recommendations below target consolidation and targeted extension points, not a rewrite.

| #   | Theme         | Recommendation                                                                               | Type     | Effort |
| --- | ------------- | -------------------------------------------------------------------------------------------- | -------- | ------ |
| P1  | Surface       | Rename `otlp4j-transport-spi` → `otlp4j-transport-common`; document it                       | Simplify | S      |
| P2  | Extensibility | Add general `Connector<I,O>` and async `Processor<T>` SPI; keep `Transform` as sync sugar    | Design   | M      |
| P3  | Lifecycle     | Introduce minimal internal wiring graph (JGraphT) subsuming `owns`, fan-out, and refcounting | Design   | M/L    |
| P4  | Robustness    | Global admission control (in-flight bytes/count limiter) for the receiver                    | Harden   | M      |
| P5  | Boilerplate   | Remove ~4× builder-delegation duplication in transport entry points                          | Simplify | S      |

---

## 2. Codebase overview

~9,500 lines of main source across 8 JPMS modules (plus generated proto):

| Module                                      | Role                                                                                               |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `otlp4j-model`                              | Immutable OTLP domain records (17 public types); JDK-only, no proto                                |
| `otlp4j-api`                                | Pipeline DSL, processors, config, receiver, `spi` (28 public types)                                |
| `otlp4j-transport-spi`                      | Shared transport impls (`ClientExporter`, `ServerReceiver`, `SignalSource`, tap); **undocumented** |
| `otlp4j-codec`                              | model/proto marshalling plus result mapping                                                        |
| `otlp4j-proto`                              | Generated OTLP messages/services                                                                   |
| `otlp4j-transport-grpc` / `-http`           | OTLP/gRPC (~900 LOC) and OTLP/HTTP (~1,040 LOC)                                                    |
| `otlp4j-samples` / `-testing` / `-coverage` | E2E demo, fixtures, aggregate coverage                                                             |

Build posture to preserve: `maven.compiler.release=25`, `failOnWarning=true`, `-Xdoclint:all,-missing`, Javadoc `failOnWarnings=true`, JaCoCo 0.90/0.80.

---

## 3. Architecture assessment

### Strengths (keep)

- **Module boundaries** — strictly one-way (`transport` → `api` → `model`, `transport` → `codec` → `proto`), enforced by `module-info` qualified exports. HTTP-only deployments never pull gRPC/Netty.
- **Immutable model** — copy-modify helpers, `Metric.NoData` instead of null, defensive `byte[]` cloning, construction-time ID/flag validation in `Ids`.
- **`Sink` / `ConsumeResult` core** — uniform across receiver, transform, exporter, batcher, and connector; wire mapping (`SignalResponses`/`DeliveryResults`) is clean: `Accepted` leaves `partial_success` unset, `Partial` carries rejected count, whole-batch `Rejected` maps to transport errors (retryable → gRPC `UNAVAILABLE` / HTTP `503`).
- **Shared resilience** — Resilience4j retry across gRPC and HTTP with jittered defaults; server retry hints (`Retry-After`, gRPC pushback) folded into client retry intervals.
- **`BatchDrainEngine`** — coalescing, serial, mutex-guarded tail chaining.
- **`ClientExporter`** — reference counting plus `Cleaner` leak watch; `@NullMarked` throughout.

### Consumer contract vs Collector

|                    | Collector                                      | otlp4j                                                          |
| ------------------ | ---------------------------------------------- | --------------------------------------------------------------- |
| Contract           | `ConsumeTraces(ctx, td) error` (sync)          | `Sink<T>.consume(T)` → `CompletionStage<ConsumeResult>` (async) |
| Result granularity | `error` or `nil`                               | `Accepted` / `Partial(n,msg)` / `Rejected(retryable,cause)`     |
| Partial success    | wire-edge only                                 | in-band through pipeline                                        |
| Cancellation       | `context.Context` per call                     | none                                                            |
| Buffer ownership   | callee must not retain; may mutate if declared | records immutable, retain/share freely                          |

**Caveats:**

- **Partial semantic drift** — after filter/transform, downstream `Partial(n)` counts may not map to upstream item identity. Bound or document this; gateways should not forward downstream per-item `Partial` upstream verbatim once batches are reshaped.
- **No per-call cancellation** — slow synchronous `Transform` on dispatch threads has no cooperative cancellation (mitigated by virtual-thread executors today).
- **No pipeline-level throttle delay** — transport clients honor server retry hints, but `ConsumeResult.Rejected` carries no delay field, so a gateway shedding load at ingress cannot tell upstream senders how long to wait (Collector propagates gRPC `RetryInfo` → HTTP `Retry-After` via `exporterhelper.NewThrottleRetry`).

### Pipeline model vs Collector graph (central gap)

`Pipeline.from(source).transform(...).filter(...).to(terminal)` is a **per-signal linear chain** with explicit `FanOut.of(...)` and manual `owns(...)`. The Collector's `service/internal/graph` dedups shared instances, inserts fan-out automatically, derives topological start/stop, and detects connector cycles.

otlp4j reproduces slices by hand: callers must order shutdown themselves, wrap multi-consumer attach in `FanOut` (single attachment slot; second `subscribe` throws — already documented in `Source`/`SignalSource` javadoc and exception message), and reason about auto-drained vs `owns(...)` resources. The ownership model and missing graph are the same problem.

**Decision (P3):** introduce a minimal internal graph (JGraphT `DirectedAcyclicGraph`) that subsumes `owns`, fan-out, and reference counting. Keep the linear DSL as the user-facing front door that compiles down to it. Do not add a heavyweight factory/registry graph.

### Processing, batching, lifecycle

- **`Transform<T>`** is sync 1-to-1 on the dispatch path. I/O-bound enrichment requires implementing `Sink` directly. Add async `Processor<T>` as a first-class extension point (P2).
- **`BatchingProcessor`** is a pipeline stage (Collector's legacy `batchprocessor` shape, not current exporter-queue direction). `consume` enqueues and returns `Accepted` immediately; downstream failures surface only via `BatchDeliveryException` on drain/logs. A fast-failing downstream keeps the queue near-empty, so overflow paths never fire and data is lost silently with zero upstream signal. Document prominently; consider a mode reflecting sustained downstream failure back as retryable at ingress.
- **Lifecycle rules** — terminals/fan-out peers implementing `AutoCloseable` auto-drain; exporter facets carry whole-exporter lifecycle; count connectors cascade downstream; `BatchingProcessor` drains its queue but **not** its downstream (requires `owns(...)`); bare-lambda terminals hide their exporter (requires `owns(...)`); shutdown order (receiver, then subscription) is caller responsibility.

### Comparison snapshot

| Concern           | Collector                          | otlp4j                                      | Assessment                       |
| ----------------- | ---------------------------------- | ------------------------------------------- | -------------------------------- |
| Mutation safety   | `MutatesData` + clone-once fan-out | immutable records                           | **otlp4j simpler**               |
| Wiring            | declarative graph, topo start/stop | linear DSL + `FanOut` + manual `owns`       | **Collector stronger; core gap** |
| Connectors        | 3×3 signal matrix, general         | 2 hardcoded counts                          | **otlp4j much narrower**         |
| Batching          | in exporter queue                  | pipeline stage                              | acceptable; note ack trade-offs  |
| Admission control | `memory_limiter` + queue bounds    | per-request size cap + per-conn concurrency | **no global limiter (P4)**       |
| Config            | shared `configgrpc/http/tls/retry` | shared `ClientConfig/ServerConfig/Tls`      | good parity                      |

vs Java SDK: `CompletionStage<ConsumeResult>` over `CompletableResultCode`; records over AutoValue; JSpecify over JSR-305; Resilience4j `RetryConfig` exposed directly; env-only config (no sysprops/per-signal overrides) — deliberate determinism.

---

## 4. Recommendations

Severity: **[H]** hardening, **[S]** simplification, **[D]** design/extensibility, **[N]** naming/docs.

### A. Structural & naming

**A1 [N][S] Two "SPI" concepts collide.** `dev.nthings.otlp4j.spi` (transport contracts: `OtlpClient`, `OtlpServer`, `Dispatchers`) vs `otlp4j-transport-spi` module / `dev.nthings.otlp4j.transport.spi` (shared implementations: `ClientExporter`, `ServerReceiver`, `SignalSource`). Rename module to `otlp4j-transport-common`, package to `dev.nthings.otlp4j.transport.common`. Effort: S.

**A2 [N] Document `otlp4j-transport-common`.** Absent from README, `architecture.md`, and `public-api.md` despite being a publicly exported module every transport depends on. Effort: S.

**A3 [S] Builder-delegation duplication.** Four near-identical wrappers (gRPC/HTTP exporter builders: 10/12 delegating setters; receiver builders: 8/7). Factor a shared abstract base keeping per-transport fluent API. Effort: S/M.

**A4 [N] Contract inconsistency in `Pipeline.Stage.transform`.** Javadoc says "returns null rejects the batch" but `Transform.apply` requires non-null; null hits `requireNonNull`, throws NPE, caught and remapped to permanent `Rejected`. Meanwhile a `filter` that drops a batch returns `Accepted`. State one intended contract and make the mechanism explicit. Effort: S.

### B. API design & extensibility

**B1 [D] General `Connector<I,O>`.** Minimal interface: `Sink<I>` emitting `O` to downstream `Sink<? super O>` with lifecycle cascade. Keep span-count and log-record-count as built-in instances. Effort: M.

**B2 [D] Async `Processor<T>` SPI.** First-class async/`CompletionStage` processing alongside `Connector<I,O>`; `Transform<T>` stays sync sugar. Effort: M.

**B3 [S] `FanOut` collapse.** Single-peer returns peer directly; empty returns documented no-op sink (match otel-java `composite`). Effort: S.

### C. Hardening

**C1 [H] Pipeline-level throttle propagation.** Add optional retry-after hint to `ConsumeResult.Rejected` so downstream backpressure propagates upstream. Effort: S/M.

**C2 [H] Global admission control (P4).** Per-request size cap (`maxInboundMessageSizeBytes`, default 4 MiB) and optional per-connection concurrency (`maxConcurrentCallsPerConnection`) plus bounded `setServerExecutor` are partial mitigations. Missing: global byte-aware bound on by default (Collector `memory_limiter`-style). Effort: M.

**C3 [H] Batching backpressure.** Document immediate-ack semantics and fast-fail silent-loss mode (see §3). Consider sustained-failure reflection mode. Effort: S (docs) / M (mode).

**C4 [H] Header validation.** `Transports.resolveHeaders` merges without validating null values or transport-illegal characters. Validate at build/attach. Effort: S.

**C5 [H] Tap `BLOCK` policy.** `OverflowPolicy.BLOCK` on `TelemetryTap` back-pressures the receiver publish thread, violating the tap's "outside the ack path" guarantee. Split enum or policy type to remove `BLOCK` for taps. Effort: S.

**C6 [H][S] Per-instance platform threads.** Each `BatchingProcessor` allocates two platform threads (timer + drain executor); each `ClientExporter` allocates one shutdown executor. At scale (N batchers, M exporters → ~2N+M idle threads), this conflicts with the virtual-thread-first dispatch model. Consolidate onto shared scheduled/drain/shutdown executors. Effort: S/M.

### D. Model fidelity

**D1 [D] Hex-string trace/span IDs.** Validated hex `String`s in `Ids` — ergonomic but heap-allocating on hot paths. Offer `byte[]`/`long`-pair representation if profiling warrants. Measure first. Effort: M.

**D2 [N] Profiles `v1development`.** Correctly `@Experimental` with opaque passthrough. Track stable schema; keep "forward 1:1, don't batch across distinct dictionaries" guidance. No action.

---

## 5. Target shape & sequencing

### Extension points for a Collector-class foundation without Collector weight

1. **Wire transport SPI** — `OtlpClient` / `OtlpServer` / `Dispatchers` (fix naming in A1/A2).
2. **`Processor<T>`** — async processing; `Transform<T>` as sync sugar (B2).
3. **`Connector<I,O>`** — general signal-changing bridge; counts as instances (B1).
4. **`Exporter` / `Sink` + `Lifecycle`** — terminal contract.
5. **Shared resilience layer** — retry/timeout; queueing/batching toward exporter edge is a later decision.

**Keep out** to stay simple: no `ServiceLoader`/factory/registry; no `MutatesData`; no YAML config engine — fluent DSL is the front door, internal graph (P3) is a wiring detail.

### Sequencing

1. **Quick wins:** A1–A4, B3, C3-docs, C4, C5.
2. **Hardening:** C1, C2, C6.
3. **Extensibility:** B1, B2.
4. **Larger:** P3 (internal graph), D1 (only if measured).
