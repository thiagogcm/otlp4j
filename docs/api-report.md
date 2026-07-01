# otlp4j API Assessment

A qualitative review of the user-facing API abstraction model, aimed at cleanup and
simplification without introducing concepts that would surprise someone already fluent in
OpenTelemetry, and while staying idiomatic to modern Java.

## Method

Five independent specialist lenses evaluated the public surface, each grounded in the actual
code plus the two reference projects, then debated the conflicts:

- **Collector alignment** — every otlp4j concept mapped to its OpenTelemetry Collector
  counterpart (`consumer`, receiver/processor/exporter/connector, `fanoutconsumer`,
  `batchprocessor`, `consumererror`).
- **otel-java idiom** — builder conventions, config surface, result/TLS/retry shapes, env
  loading, measured against `exporters/otlp`, `sdk/common`, and autoconfigure.
- **Modern Java** — records, sealed types, pattern matching, `Flow`, `CompletionStage`, JPMS,
  functional-interface ergonomics, nullness.
- **Surface minimalism** — a concept census and a hunt for redundancy and one-off abstractions.
- **Lifecycle & ownership** — the `owns(...)` / facet-shared-lifecycle / drain model against the
  Collector's reverse-topological shutdown and otel-java's cascading `shutdown()`.

Severities: **High** = correctness or a real ecosystem-parity gap; **Med** = a learnability or
consistency cost; **Low** = polish. "Blast" notes the breaking-change reach. Breaking changes are
in scope by design.

## Baseline: the surface today

`otlp4j-api` exports **7 packages / 30 top-level public types**; `otlp4j-model` adds 18; the two
transports add 4 entry points. About **52 top-level public types across ~10 importable packages**.

Concepts a newcomer must hold to be productive, per core task:

| Task | Concepts |
| --- | --- |
| Receive and print | 3 — `OtlpGrpcReceiver`, `ConsumeResult`, `TracesData` |
| Receive, transform, export | 8 — + `Source` (`receiver.traces()`), `Pipeline`/`Stage`, `Transforms`, `OtlpExporter` facet, `PipelineHandle`, `Lifecycle`/`owns` |
| Construct and export | 7 — `TracesData`, `Resource`, `InstrumentationScope`, `Attributes`, `AttributeValue`, `Span`, exporter facet |

Deduplicated, **~15 concepts** to be fluent. The recommendations below take the API toward
roughly **5–6 packages / ~21 top-level types** with no capability loss, and remove one real
correctness hazard.

The core abstraction model is sound and largely well-aligned; most findings are cleanups, a few
are ecosystem-parity gaps, and one is a bug. The [Keep as-is](#keep-as-is) section is deliberately
long — the shape is good and should not be over-churned.

## Executive summary

Highest leverage first:

1. **Fix the shared-exporter shutdown hazard** (A1) — a real correctness bug: attaching two
   facets of one exporter to two subscriptions lets one `shutdown()` close the channel under the
   other.
2. **Batching knobs count batches, not items** (E1) — silently 512× off from a Collector user's
   `send_batch_size` mental model. Name the unit or count items.
3. **Retries default OFF; otel-java defaults them ON** (B1) — a silent, one-line resilience
   divergence.
4. **Merge the single-purpose `connector` and `exporter` packages** (D1, D2) — two of seven API
   packages carry one/two types with no extension point; fold them in, keep the vocabulary.
5. **Make hidden downstreams cascade their own lifecycle** (A2) — remove the main reason `owns(...)`
   exists at all.
6. **Close the TLS and header capability gaps** (B2, B3) — no in-memory/`SSLContext` TLS, no
   `Supplier` headers for token rotation; both are things otel-java users reach for.
7. **Drop the phantom `ConsumeResult<T>` type parameter** (D5) — it guards nothing and forces
   unchecked casts the code already documents as such.
8. **Unify naming outliers** (C1–C4) — the builder `set*`/no-prefix split, `transport(config)`,
   `getDefault()`, and the `on(port)`/`onTraces` collision.

Suggested sequencing is in [Tiers](#suggested-sequencing).

---

## A. Lifecycle, ownership, and shutdown

The most novel and cognitively loaded area, and where the only correctness bug lives. The Collector
owns its topology and shuts it down in reverse-topological order (`service/internal/graph`
`Graph.ShutdownAll`); otel-java cascades `SdkTracerProvider.shutdown()` into every processor and on
to the exporter (`BatchSpanProcessor` → `spanExporter.shutdown()`). otlp4j instead asks the user to
hand-sequence two independent `Lifecycle`s and to declare invisible resources with `owns(...)`.

### A1 — Shared-exporter shutdown closes the channel out from under another subscription — High

`OtlpExporter`'s facets carry the exporter's lifecycle: "draining any facet drains the whole
exporter." Convenient for one pipeline, but nothing tracks multiplicity. A normal split-signal
topology —

```java
Pipeline.from(receiver.traces()).to(exporter.traces());     // subscription A
Pipeline.from(receiver.metrics()).to(exporter.metrics());   // subscription B
```

— breaks on shutdown: `A.shutdown()` drains the whole exporter and closes the shared channel, so
`B`'s metrics facet now delivers to a dead channel, and the GC-leak backstop never fires because the
channel *was* closed. The Collector and otel-java avoid this by having the container own each
component and shut it down exactly once.

- **Fix (minimum):** reference-count the shared exporter (or scope-guard the channel) so a facet
  shutdown closes the channel only on the last release. Zero new types, fixes the bug.
- **Fix (fuller):** the `Gateway` aggregator in A3, which owns each exporter once by construction.
- **Blast:** internal; behavior becomes correct where it is currently wrong.

### A2 — `owns(...)` exists mainly because connectors erase their own downstream — High

The only non-trivial `owns(...)` case in the docs is the count connector: users must write
`.owns(metricsExporter)` because "the subscription cannot see through a count sink to its
downstream." That blindness is self-inflicted — `Connectors.spanCount` returns `counter::consume`, a
method reference that erases the `CountConnector`, and `CountConnector` is not itself `Lifecycle`, so
the pipeline's lifecycle collector walks past its captured downstream. Both reference
implementations reach the downstream automatically because wiring is a typed reference, not an opaque
closure.

- **Change:** make `CountConnector implements Lifecycle`, cascading `shutdown`/`forceFlush` to its
  downstream; have `Connectors.spanCount`/`logRecordCount` return the connector itself (still a
  `Sink` facet, now also `AutoCloseable`). The pipeline then drains the connector's downstream like
  any peer, and the `.owns(metricsExporter)` line disappears from the docs.
- **Blast:** removes a documented step; behavior becomes "attach the connector, its downstream
  drains" — the same rule as facets.

### A3 — No topology object; the "shared budget" silently excludes the receiver — High → optional new type

`Pipeline.from(receiver.traces())` takes a `Source`, never the `Receiver`, so the returned
`PipelineHandle` cannot drain ingest. The "one shared budget" claim is only half true: the
subscription's budget covers terminals and peers, while the actual ingest drain is a *separate*
`receiver.shutdown(timeout)` with its own deadline. Two calls, two budgets, ordering by prose.

- **Change:** add a small, **optional** `Gateway` (topology) aggregator that owns receiver +
  subscriptions + exporters and shuts them down once, in Collector order:

  ```java
  var gateway = Gateway.of(receiver)      // Gateway is itself a Lifecycle
          .exporter(exporter)             // register shared exporters once (the dedup point)
          .pipeline(subA).pipeline(subB);
  ...
  gateway.close();  // stop ingest, drain subscriptions, close each exporter exactly once
  ```

- **Condition for it to be a net win** (both the minimalism and lifecycle reviewers agree): it must
  *replace*, not supplement. It has to retire `owns(...)` from the common path, retire the two-call
  ordering prose, and narrow the "facets silently carry the exporter's lifecycle" rule that causes
  A1. If those stay, you have two lifecycle models — net bloat. Hello-world (one receiver → one
  exporter → one subscription) must stay `Gateway`-free.
- **If the team will not also retire the facet-lifecycle magic:** skip `Gateway` and take the A1
  ref-count fix alone.
- **Blast:** additive; the two-call form keeps working during migration.

### A4 — Documented shutdown order is the reverse of the Collector's — Med

The docs prescribe "shut down the subscription first … then the receiver." Subscription-shutdown
detaches the sink and tears down the exporter *while the receiver is still accepting*, so for the
whole drain window live senders get a retryable `Rejected` → `UNAVAILABLE`/`503`. The Collector
stops in topological order on purpose, "so that each component has a chance to drain to its consumer
before the consumer is stopped" — receivers first, exporters last.

- **Change:** flip the recommended order to receiver-first (`receiver.shutdown` drains in-flight
  requests through the still-live exporter; detach is instantaneous, so nothing is lost by closing
  the subscription second), or bury ordering inside the A3 `Gateway` so users never sequence it.
- **Blast:** doc/example edit; both orders currently "work," so no code breaks.

### A5 — `close()` grace period is inconsistent with its own contract — Med

The docs promise "a fixed ten-second default across the receiver, exporter, and subscription."
`Lifecycle.close()` honors 10s, but `BatchingProcessor.close()` overrides to **2s** (and swallows
failures) and `OtlpServer.close()` uses **30s**. Three grace periods behind one `AutoCloseable`
contract surprises anyone using try-with-resources, and the 2s batcher close can drop a partly-full
queue 8s early against a slow collector.

- **Change:** honor the 10s default everywhere, sourced from one named constant; if the batcher
  genuinely needs a snappier close, make that explicit and documented, not incidental.
- **Blast:** internal.

### A6 — `Lifecycle.close()` blocks on `join()` — Low

`close()` does `shutdown(...).toCompletableFuture().join()`. Idiomatic (otel-java's
`SdkTracerProvider.close()` does the same), but calling it from a pipeline/completion thread the
drain itself needs can stall a carrier until the deadline. One sentence in the thread-safety section
("don't call `close()` from a completion thread; use `shutdown(Duration)` and compose") covers it.

---

## B. Ecosystem-behavior parity (otel-java)

These are the gaps an otel-java user hits without warning. The abstraction shapes are fine; the
defaults and escape hatches are not.

### B1 — Retries default OFF; otel-java defaults them ON — High

`ClientConfig.Builder.retry = RetryPolicy.none()`. Both `GrpcExporterBuilder` and
`HttpExporterBuilder` in otel-java default `retryPolicy = RetryPolicy.getDefault()` — retries on out
of the box. The irony: otlp4j's own `RetryPolicy.getDefault()` values are byte-identical to
otel-java's (5 attempts, 1s→5s, 1.5×), so the intent matched; only the default binding differs, and
silently.

- **Change:** default `ClientConfig.Builder.retry` to `RetryPolicy.getDefault()`; keep
  `RetryPolicy.none()` as the explicit opt-out. If "off by default" is a deliberate stance for a
  gateway that also fronts a `BatchingProcessor` queue, make it a loud, documented choice rather
  than a quiet divergence.
- **Blast:** behavior-only; callers wanting the old behavior pass `RetryPolicy.none()`.

### B2 — TLS has no in-memory / `SSLContext` escape hatch — Med-High

The sealed `Tls` shape (`disabled`/`systemTrust`/`trust(Path)`/`custom(Path, Path, Path)`) is a good
modern-Java divergence — keep it. But it is **Path-only**: a gateway loading certs from a secret
manager or Vault cannot pass PEM bytes, and there is no bring-your-own-`SSLContext` door. otel-java
users reach for `setTrustedCertificates(byte[])`, `setClientTls(byte[], byte[])`, and
`setSslContext(SSLContext, X509TrustManager)` and find nothing.

- **Change (additive):** add `Tls.custom(byte[] cert, byte[] key, @Nullable byte[] trust)` and a
  `Tls.sslContext(SSLContext, X509TrustManager)` sealed variant. Closes the gap and gives otel-java
  users the `setSslContext` equivalent inside otlp4j's own idiom.
- **Blast:** additive; the transports' `PemSsl` paths need byte[] entry points.

### B3 — Headers are static-map only; no `Supplier` for rotating auth — Med

`addHeader`/`setHeaders(Map)` are captured once at build. otel-java's
`setHeaders(Supplier<Map<String,String>>)` is evaluated per export — the standard way to refresh a
bearer token. A long-lived gateway forwarding to an auth'd backend cannot rotate credentials without
rebuilding the exporter.

- **Change (additive):** add `setHeaders(Supplier<Map<String,String>>)`, threaded into
  `ClientConfig`; keep the static overload for the common case.
- **Blast:** additive. (If dynamic egress auth is deemed out of scope for a gateway that terminates
  auth in a sidecar, document that decision explicitly.)

### B4 — `fromEnvironment()` is env-only and order-dependent — Med

The opt-in, deterministic model is a *good* divergence from autoconfigure's magic — keep it. Two
rough edges: it reads `System.getenv` only (an otel-java user will expect
`-Dotel.exporter.otlp.endpoint` system properties to work), and the "call `fromEnvironment()` first
or it clobbers your setters" ordering is a footgun autoconfigure does not have (it is a separate
phase).

- **Change:** make env values lowest precedence regardless of call order (buffer them, apply at
  `build()` only where the setter was untouched), which kills the ordering trap and is strictly
  safer. Document the env-only scope prominently. System-property support optional.
- **Blast:** precedence change is behavior-only and safer.

### B5 — No `setConnectTimeout` — Low

Only `setTimeout(Duration)` (per-request). Dropping otel-java's `(long, TimeUnit)` overloads is
correct (Duration is the modern idiom). A `setConnectTimeout(Duration)` is a small, additive gap for
flaky-network gateways; optional.

---

## C. Naming and convention consistency

### C1 — Builder setter naming is split with no rule — Med

Model and processor builders are no-prefix (`Metric.Builder.name`, `NumberPoint.Builder.epochNanos`,
`BatchingProcessor.Builder.flushThreshold`); config and transport builders are `set*`
(`ClientConfig.Builder.setTimeout`, `OtlpGrpcExporter.Builder.setEndpoint`). A user writing
`NumberPoint.builder().epochNanos(x)` then `ClientConfig.builder().setTimeout(y)` feels the seam. The
JDK's own fluent builders (`HttpClient.Builder`, `HttpRequest.Builder`) are no-prefix; otel-java is
`set*`.

- **Change:** pick one rule and document it. The split is semi-principled (data construction vs
  configuration) — if kept, state it explicitly ("model builders no-prefix, config builders `set*`
  to match otel-java"). Otherwise standardize; no-prefix matches the records and the JDK, `set*`
  matches the ecosystem the docs court.
- **Blast:** rename + deprecate old names for a release.

### C2 — `transport(config)` breaks the `set*` convention — Med

`OtlpGrpcExporter.Builder.transport(ClientConfig)` (and the receiver's `transport(ServerConfig)`) is
the one noun method on an otherwise `set*` surface, and it reads like an accessor. It also exposes
that the entry-point builder and `ClientConfig.Builder` are near-duplicate parallel builders.

- **Change:** rename to `setConfig(...)` (or offer a `builder(ClientConfig)` overload). Keep
  `ClientConfig`/`ServerConfig` public — they are load-bearing for the `OtlpClient`/`OtlpServer` SPI
  — just make the door consistent.
- **Blast:** mechanical rename.

### C3 — `getDefault()` is the lone getter-prefixed name — Low

The API is otherwise getterless (`port()`, `data()`, `peers()`, `host()`), yet
`ClientConfig.getDefault()`/`ServerConfig.getDefault()`/`RetryPolicy.getDefault()` use the bean
prefix, while `TapOptions.defaults()` spells the same idea the other way. Rule of thumb from the
debate: **name for parity where a real otel-java twin exists; house style everywhere else.**

- **Change:** keep `RetryPolicy.getDefault()` (byte-for-byte twin of
  `io.opentelemetry.sdk.common.export.RetryPolicy.getDefault()` — a migrating user types exactly
  that). Rename `ClientConfig.getDefault()`/`ServerConfig.getDefault()` → `defaults()` (they have no
  otel-java counterpart, so parity is a non-reason; match `TapOptions.defaults()`). Not `of()` —
  that implies arguments.
- **Blast:** rename + deprecate.

### C4 — Terse `to`/`on` factories collide and multiply the endpoint surface — Med

`OtlpGrpcReceiver.on(4317)` (static bind factory) sits right beside `.onTraces(sink)` (builder
callback) — the same `on` prefix for "bind here" and "handle this signal." Separately, the exporter
endpoint has ~5 spellings (`to(host,port)`, `setEndpoint(url)`, `setEndpoint(host,port)`, `setHost`,
`setPort`) where otel-java has one canonical `setEndpoint(String url)`.

- **Change:** keep `builder()` prominent and `setEndpoint(String url)` canonical. Keep one
  ergonomic shortcut — `OtlpGrpcExporter.to(host, port)` reads well and compounds when wiring many
  exporters. Rename or drop the receiver's `on(...)` to remove the `onTraces` clash (keep
  `ephemeralPort()` + `builder()`), and consider dropping standalone `setHost`/`setPort` (they
  interact confusingly with URL parsing, which sets host+port+scheme+path at once).
- **Blast:** remove/rename secondary factories; `setEndpoint`/`to` remain.

### C5 — Alias singular/plural drift — Low (optional)

`TraceSink` consumes `TracesData` via `receiver.traces()` — singular type name against plural
everything-else. If the `*Sink` aliases stay (they should — see D3), renaming them to
`TracesSink`/`MetricsSink`/`LogsSink`/`ProfilesSink` costs nothing, has no collision, and restores
consistency. Pure polish; skippable.

### C6 — Redundant singleton spellings, inconsistently visible — Low

Two public doors to one value, done three different ways: `Metric.NoData.INSTANCE` *and*
`Metric.noData()`; `ConsumeResult.Accepted.instance()` *and* `ConsumeResult.accepted()`;
`AttributeValue.EMPTY` *and* `empty()` — while `Tls` hides `Disabled.INSTANCE` behind `disabled()`
(the clean version).

- **Change:** keep only the factory (`noData()`, `accepted()`, `empty()`); make the
  `INSTANCE`/`instance()` singletons private, per the `Tls` precedent. Pattern-match arms use
  `case Metric.NoData ignored`, which needs no public constant.

---

## D. Surface minimalism

### D1 — `connector` is a whole package for two count factories — Med-High (merge)

The entire exported `connector` surface is `Connectors` (two static methods,
`spanCount`/`logRecordCount`) + `FailurePolicy`; `CountConnector` is package-private. Both the
Collector-alignment and minimalism reviewers agreed after debate: the Collector's `connector`
package earns standalone status because it is an *extensibility contract* (a public
`connector.Factory` you implement to bridge two pipelines). otlp4j exposes none of that — there is no
public `Connector` interface, and by otlp4j's own convention closed built-in families already live in
`processor` (`Transforms`, `BatchingProcessor`, `OverflowPolicy`).

- **Change:** move `Connectors` + `FailurePolicy` into `dev.nthings.otlp4j.processor`; delete the
  `connector` package. **Preserve the vocabulary** — keep the class named `Connectors`, keep the
  `otlp4j.connector.span.count` / `otlp4j.connector.log.record.count` metric names, keep "connector"
  in the prose. The signal-changing nature (trace→metric, unlike a same-signal `Transform`) survives
  in docs.
- **Capability lost:** none. One fewer package.
- **Escape clause:** if a public `Connector` SPI is genuinely planned for the next release or two,
  keep the thin package now to avoid churning it twice. A thin package for a *real, imminent*
  extension point is legitimate; for a hypothetical one it is not. At `0.1.0` with no such interface
  in sight, merge.

### D2 — Single-type `exporter` package, asymmetric with `receiver` — High (merge)

`exporter` exports exactly one interface, `OtlpExporter`, whose facets are `Sink` aliases from
`pipeline`. Its ingest mirror `Receiver` lives in `receiver` bundled with the tap types — so the two
endpoint abstractions sit in two packages, one holding a single type.

- **Change:** either fold `OtlpExporter` into `pipeline` (it already depends only on
  `Sink`/`Lifecycle` there), or create one `endpoint` package holding `Receiver` + `OtlpExporter`
  (+ the tap types). Takes the API from 7 packages to 5–6.
- **Capability lost:** none; import-path break only.

### D3 — The four `*Sink` aliases: keep them, but stop *requiring* them — Med

`TraceSink`/`MetricSink`/`LogSink`/`ProfileSink` are empty marker interfaces. They mirror the
Collector's per-signal `consumer.Traces`/`Metrics`/`Logs` and read well at declaration sites, so
they earn their keep as **optional lambda-target sugar** — the removal camp lost this one on the
readability/familiarity argument. The real cost is an interop tax: a plain `Sink<TracesData>` is
*not* a `TraceSink`, so code holding the general type must adapt with `::consume` (visible in
`Connectors.spanCount`, which returns `counter::consume`).

- **Change:** type **parameters** as `Sink<? super TracesData>` (a lambda, a `TraceSink`, or a raw
  `Sink<TracesData>` all fit); keep the aliases only as optional return/declaration sugar. Add
  `@FunctionalInterface` to the base `Sink` too, so an accidental second abstract method can't
  silently break every lambda call site.
- **Blast:** widening a parameter type is source-compatible for callers.

### D4 — `tap().all()` + the `Telemetry` envelope cost 5 types for one convenience — Med

The observe surface is really one type, `TelemetryTap`, plus `TapOptions` and the sealed `Telemetry`
(+ `Traces`/`Metrics`/`Logs`/`Profiles`), which exist solely for `all()`. The "three ways to
observe" in the docs are one type plus two usage idioms (a fan-out observer sink and an identity
`Transform` are patterns, not new API).

- **Change:** drop `all()` and delete `Telemetry` + its four records; keep the four typed
  `Flow.Publisher`s. Separately, fix the racy `setOptions`-then-`subscribe` design by binding options
  at subscribe time (`tap().traces(TapOptions)`), so config attaches to the subscription instead of
  tap-wide mutable state.
- **Capability lost:** a single unified all-signals stream, reconstructable by subscribing to the
  four. Low-confidence cut — `all()` + an exhaustive `switch` is handy for a one-place debug sink; if
  kept, at least document `Telemetry` as just a tagged union of the four `*Data` types.
- **Optional (Collector alignment):** the tap has no Collector analogue at all; consider moving
  `tap()` off the core `Receiver` interface to an opt-in accessor so the default mental model stays
  receiver → consumer → pipeline.

### D5 — Drop the phantom `ConsumeResult<T>` type parameter — Low (cleanup)

Settled in debate by both the modern-Java and otel-java reviewers. No variant carries a `T`
(`Accepted()`, `Partial(long, String)`, `Rejected(boolean, String, Throwable)`), so
`ConsumeResult.<AnySignal>accepted()` mints freely and the "prevents cross-signal merges" guard is
illusory. The library already relabels the tag across signals with unchecked casts — `FanOut.retag`
is documented "sound because ConsumeResult's type parameter is phantom," and `CountConnector.applyPolicy`
deliberately carries a `ConsumeResult<MetricsData>`'s message/cause onto a trace/log result, which
compiles *because* the tag is meaningless. The only real merger, `fanOutMerge`, reads only
`retryable`/`rejectedItems`/`message`/`cause`, never `T`.

- **Change:** make `ConsumeResult` non-generic: `Sink<T>.consume(T)` returns
  `CompletionStage<ConsumeResult>`. Deletes `Pipeline.retag`, `FanOut.retag`, and the
  `Accepted.instance()` cast, and collapses `applyPolicy` to a pass-through. `Sink<T>` keeps its own
  `<T>`, so input safety (can't feed `MetricsData` to a `TraceSink`) is fully preserved — only the
  result wrapper loses a tag no variant ever held. Keep the sealed three-variant hierarchy and the
  retryable/permanent modeling exactly as they are.
- **Blast:** wide but mechanical (`Sink`, the 4 aliases, the `OtlpClient` SPI signatures, any user
  `ConsumeResult<X>` annotations). Bundle into a breaking pass; do not break for it alone.

### D6 — Smaller demotions — Low

- **`ThrowingConsumer`** is public but exists only as the parameter of `Sink.accepting(...)`. Nest it
  as `Sink.ThrowingConsumer` to shrink the `pipeline` census by one.
- **`PipelineHandle`** adds no members over `Lifecycle` (it re-declares `shutdown(Duration)` with a
  doc tweak). Either return `Lifecycle` directly, or keep it as a deliberate documentation-bearing
  noun ("the subscription you close") — a low-value cut, author's call.
- **Two parallel builder surfaces** (`OtlpGrpcExporter.Builder` vs `ClientConfig.Builder` expose the
  same setters) teach the config vocabulary twice. No type need be removed, but make the entry-point
  builder the sole documented path and have it delegate to `ClientConfig.Builder` rather than
  restating it.

---

## E. Collector-model alignment

### E1 — Batching knobs count batches where the Collector counts items — High

`BatchingProcessor.Builder.flushThreshold(int)` and `queueCapacity(int)` count **batches**; the
Collector's `batchprocessor` `send_batch_size`/`send_batch_max_size` count **items**. A Collector
user reads `flushThreshold(512)` as "flush at 512 spans" but gets "flush at 512 *batches*" —
potentially orders of magnitude more volume. `queued()`/`droppedCount()` also count batches. The
docs do say so, but documented surprise is still surprise for this persona.

- **Change:** either make `flushThreshold` count **items** (align with `send_batch_size`, using the
  existing per-signal item count), or — if batch granularity is deliberate for cheaper counting —
  rename the knobs so the unit is unmistakable: `flushAfterBatches(int)` / `maxQueuedBatches(int)`.
  The rename is the safe minimum; item semantics is the fuller alignment.
- **Blast:** switching to item semantics is a silent behavior change (needs a changelog callout);
  renaming is a one-line compile break.

### E2 — `Source` single-attachment vs the Collector's implicit fan-out — Med (keep, noted)

otlp4j hands you a `Source`, you `subscribe` one consumer, and a second subscriber throws
`IllegalStateException` — forcing explicit `FanOut`. The Collector inverts this: listing a receiver
in several pipelines makes the service build the fan-out automatically. This is a defensible Java
divergence (it keeps merge semantics and lifecycle explicit, which matters because otlp4j exposes
wiring to end users where the Collector hides it behind config), but it is a concept to learn and the
"second consumer throws" is a small surprise. Optionally, allowing repeated `subscribe` to
auto-fan-out would match the Collector and remove the surprise; weigh against losing the visible
merge point. Recommendation: keep explicit fan-out, document the single-slot rule up front.

### E3 — "facet" is invented prose vocabulary — Low

The per-signal `traces()`/`metrics()`/… views are called "facets" throughout the docs. The *shape*
(per-signal consumer views over one connection) is fine and Collector-reasonable; only the word is
novel. In prose, "the per-signal export consumers" (or "per-signal exporters") is more familiar. No
type changes — documentation only.

---

## Where the reviewers disagreed

Four conflicts surfaced and were resolved by debate:

- **`connector`: merge vs keep the package.** Resolved → **merge into `processor`, preserve the
  vocabulary** (D1). The Collector-alignment reviewer conceded that a package should signal an
  *extension point*, and there is none here.
- **`Sink` → `Consumer` rename.** Resolved → **keep `Sink`.** The decisive correction: "consumer" is
  Collector-Go vocabulary, not OpenTelemetry-wide (otel-java uses `SpanExporter`/`SpanProcessor`), so
  the rename would buy Collector familiarity at the cost of a `java.util.function.Consumer` collision
  and otel-java-user surprise. The Collector bridge is already carried by the method `consume(...)`
  and the return `ConsumeResult`.
- **`ConsumeResult<T>`: keep for symmetry vs drop as phantom.** Resolved → **drop** (D5). The
  otel-java reviewer withdrew the "nice touch" defense once the `CountConnector.applyPolicy`
  cross-signal relabel and the `FanOut.retag` "phantom" comment were on the table.
- **`Gateway` vs minimal fix.** Resolved → **fix the A1 hazard regardless**; adopt `Gateway` (A3)
  only if it *replaces* `owns(...)`, the ordering prose, and the facet-lifecycle magic. Otherwise
  reference-count the exporter and stop there.

## Keep as-is

The shape is good. These are deliberate, well-judged choices — do not churn them:

- **Sealed `ConsumeResult` (`Accepted`/`Partial`/`Rejected`) with explicit `retryable`/`permanent`
  and optional `cause`.** The single most-justified divergence from otel-java: `CompletableResultCode`
  is success/fail only and cannot express OTLP `partial_success` or the `UNAVAILABLE`-vs-`INTERNAL`
  (503-vs-500) distinction a gateway must *produce* on the receive side. Maps cleanly to
  `consumererror` permanent/retryable. (Drop only the phantom `<T>`, D5.)
- **`CompletionStage<ConsumeResult>` over `CompletableResultCode`.** A place to diverge *because
  otel-java's shape is the awkward one* — `CompletableResultCode` predates wide `CompletableFuture`
  adoption. `CompletionStage` composes, interops, and fits virtual threads;
  `ConsumeResult.acceptedStage()` softens the common case (add `partialStage()`/`rejectedStage()` to
  match).
- **Multi-signal `OtlpExporter` + `traces()/metrics()/logs()/profiles()` facets over one channel**
  (and `Receiver` facets on the ingest side). A coherent gateway model; the documented no-per-signal-
  endpoint trade-off is the right call.
- **Sealed `Tls` model.** Cleaner than otel-java's additive byte[] trio and lets the server reject
  `SystemTrust` exhaustively. Add the escape hatch (B2); keep the shape.
- **`RetryPolicy` record** — method names and defaults (5 / 1s / 5s / 1.5×) match otel-java's
  `RetryPolicyBuilder` exactly; `none()`/`getDefault()` are good additions. The gold-standard
  alignment in the codebase (wire it as the default, B1).
- **`Compression` enum + `setCompression(String)`** — type-safe primary with a familiar string door.
- **`FanOut`** — maps cleanly to `internal/fanoutconsumer`; the immutable-records-so-no-clone stance
  is exactly the Collector's `Capabilities{MutatesData:false}` fast path; "any peer rejects →
  rejected, worst-case partial count" is a principled analogue of the Collector's error accumulation.
- **`Metric.Data` null-object (`NoData`)** so `data()` is never null, with a full sealed set — callers
  get exhaustive switches with no null branch.
- **`Flow.Publisher` tap capability** — JDK-native reactive SPI, no third-party dependency, back-
  pressure isolated from the ack path. Keep the capability (trim `all()`/`Telemetry` per D4).
- **Pipeline DSL and factory verbs** — `Pipeline.from`, `FanOut.of`, `Transforms.keepSpansWhere`,
  `BatchingProcessor.forTraces`, `Source.discard`, `ephemeralPort()`. Idiomatic JDK factory naming
  (`Stream.of`, `List.of`); no otel-java analog to match, so judged on merit.
- **`Transform<T>` + `Transforms`.** The one permitted "vanity" SAM — it anchors the
  "must not retain or mutate the batch" contract and reads better in the DSL than a raw
  `UnaryOperator<T>`. Do **not** rename it to `Processor` (that word is the umbrella kind, owned by
  the `processor` package and `BatchingProcessor`). `Stage.filter` (batch-level) vs
  `Transforms.keepSpansWhere` (item-level) are different granularities, not duplicates.
- **`Source` vs `Sink` duality**, **`OverflowPolicy`** (deliberately shared by `BatchingProcessor`
  and `TapOptions`), and the **`spi` altitude** (`OtlpClient`/`OtlpServer` for transport authors) —
  all clean. (`Dispatchers` is the one SPI tidy-up: reuse `Sink` instead of a raw `Function` bundle,
  and the plural name fits one record awkwardly.)
- **`Lifecycle` contract** — `shutdown(Duration)`/`forceFlush(Duration)`/`close()` returning
  `CompletionStage`, `shutdownNow()` vs graceful (reads like `ExecutorService`), the per-step shared
  remaining-deadline, best-effort continue-on-error teardown, and idempotent shutdown. The *shape* is
  right; only the ownership wiring (Section A) needs work.
- **JPMS boundaries** — `@NullMarked` packages, qualified proto exports to codec/transports,
  encapsulated `processor.internal`, `@Experimental`. Clean.
- **The 18 `model` records** — mirror OTLP fidelity 1:1; the point-type sprawl is the wire format's,
  not gratuitous.

## Suggested sequencing

Grouped so each tier is independently shippable.

**Tier 1 — correctness and free wins (low blast, high value):**
A1 (shared-exporter hazard, ref-count), A5 (close timeouts), B1 (retry default), E1 (batching unit —
rename at minimum), C3/C6 (naming polish), D5 (drop `ConsumeResult<T>` — fold into whatever breaking
pass carries the others).

**Tier 2 — structural cleanup (moderate blast, no capability loss):**
D1 (merge `connector`), D2 (dissolve `exporter` package), D3 (`Sink<? super T>` params + alias
sugar), A2 (cascading `CountConnector`), A4 (receiver-first shutdown order), C1/C2/C4 (builder naming
unification).

**Tier 3 — capability and bigger model changes:**
B2 (in-memory/`SSLContext` TLS), B3 (`Supplier` headers), B4 (env precedence), A3 (`Gateway`
aggregator, if it replaces the old ownership model) with D4 (trim the tap surface), E2 (revisit
`Source` fan-out).
