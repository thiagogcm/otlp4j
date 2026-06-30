# otlp4j API Report

An outside-in assessment of the user-facing API abstraction model ahead of the 1.0 freeze, with a prioritized cleanup roadmap. The goal is to simplify the surface and to stay familiar to developers who already know the OpenTelemetry Java SDK, without introducing disruptive concepts. Breaking changes are in scope: class, package, and module renames, additions, and removals are all on the table.

## Summary

otlp4j's bones are right. Its domain abstraction set (receiver/source, an acknowledged `consume`/`ConsumeResult` contract, a transform/filter/fan-out pipeline, signal connectors, a live tap, and queue batching) maps almost one-to-one onto the OpenTelemetry Collector and is load-bearing, not invented. The weaknesses are **accidental complexity and surface-idiom drift**, not a domain mismatch. So the cleanup is "remove the seams and align the idioms," not "restructure the model."

| Lens | Grade | One-line |
| --- | --- | --- |
| Domain modeling | **B+ (≈4.0/5)** | The abstraction set matches the gateway domain and is already Collector-aligned. |
| Familiarity to opentelemetry-java | **C- (≈2.2/5)** | The first three things an OTel user types (a `set*` setter, `setEndpoint(url)`, a per-signal class) all diverge. |
| Simplicity / conceptual weight | **D+ (≈1.6/5)** | ~35 public types, a `core` junk-drawer package, and a docs climax of a shutdown chapter plus an ownership cheat-sheet. |

**Recommended stance: Hybrid.** Keep the Collector-shaped domain where the SDK has no analog (receiver, pipeline, receive-side partial success). Make the surface idioms (builders, lifecycle, result semantics, naming) maximally familiar to opentelemetry-java users and as lean as possible. This is where a three-lens review converged: the domain is sound, but its clothing is half SDK, half Collector, and inconsistent with itself.

## Method

Three reviewers evaluated the real code, each with a deliberately opposing lens, against two references: the opentelemetry-java **SDK** (the familiarity baseline a consumer brings) and the OpenTelemetry **Collector** (the correct reference for the gateway half the SDK has no equivalent for).

- **Familiarity advocate**: every divergence from opentelemetry-java exporter conventions is a tax to justify or remove.
- **Minimalist**: "when in doubt, leave it out"; conceptual weight, orthogonality, footguns, and documentation-burden-as-a-smell.
- **Domain pragmatist**: defend what is legitimately gateway-shaped; veto over-correction that amputates real capability.

Findings below are consensus unless a debate resolution is noted. Each cited claim was checked against the source.

## The central tension

otlp4j is two libraries in one trench coat. The **export half** overlaps the SDK directly: an OTLP exporter with endpoint, TLS, headers, compression, retry, and a flush/shutdown lifecycle. The **gateway half** (receiving OTLP on a port, routing and fanning out batches, deriving metrics, tapping a live stream, returning partial-success on ingest) has no SDK counterpart at all; its natural reference is the Collector (`consumer.ConsumeTraces`, components, receivers, processors, connectors, exporters).

The current API blurs the two. It borrows Collector verbs (`consume`, `ConsumeResult`, `Source`/`Sink`, `Connector`) but wraps them in non-OTel builder idioms (`endpoint(host, port)`, `compression(GZIP)`), and it is inconsistent with itself (`TraceSink` consuming `TracesData`; two packages both named `spi`).

The Hybrid resolution rule used throughout this report:

> Be familiar where you overlap the SDK. Be Collector-idiomatic, self-consistent, and minimal where you don't.

Concretely: align the exporter builders, the lifecycle, and the result-construction ergonomics with opentelemetry-java; keep the receiver/pipeline/partial-success model but make its names borrow from the Collector so the novelty reads as intentional rather than invented; and delete the accidental complexity that neither reference has.

## Findings and recommendations

Severity reflects user impact (footgun or correctness > familiarity friction > cosmetic), not implementation cost.

### Tier 1: Footguns and correctness (highest leverage; all three lenses agree)

#### F1. Remove the `owns()` / Auto-Explicit-Hidden lifecycle taxonomy `[High]`

The single largest wart, and it traces to one fixable design choice.

`OtlpExporter` is **not** a `Sink`. It is a facade whose per-signal facets are bare method-reference lambdas:

```java
// transport-spi: ClientExporter.java
this.traces   = client::exportTraces;   // a plain TraceSink with no lifecycle
this.metrics  = client::exportMetrics;
```

So when a user writes `Pipeline.from(receiver.traces()).to(exporter.traces())`, the pipeline only sees a lambda; the back-reference to the exporter (which owns the channel) is lost. The pipeline **already** auto-collects any terminal or fan-out peer that is `AutoCloseable` (`PipelineLifecycle.leafResources`), which is why a directly-attached `BatchingProcessor` or a custom single-signal `Exporter<T>` drains automatically. Only the facade facets are excluded, by construction.

To paper over that, the API grew: a `Stage.owns(AutoCloseable)` method, a `to(terminal, owner)` overload, a `Cleaner`-based leak warning fired from GC, a full "Shutdown order" chapter in `public-api.md`, and a "Lifecycle cheat sheet" with a three-category ownership taxonomy (Auto / Explicit / Hidden). Documentation that elaborate around a single hazard is not teaching the domain; it is apologizing for the API. The clearest tell: the project's own sample hedges with **both** mechanisms at once:

```java
// OtlpE2eDemo.java
.owns(backendExporter)               // line 99: register for the subscription to drain
...
} finally {
    backendExporter.close();         // line 117: and close it again, defensively
}
```

When the author of the library belt-and-suspenders the lifecycle, the model is too subtle.

**Fix.** Make each facet a lifecycle-bearing view (`Drainable`/`AutoCloseable`) that delegates to the parent exporter's `shutdown`, which is already idempotent. The pipeline's existing auto-collection then drains the exporter with no extra ceremony.

```java
// Before: the channel leaks unless you remember to register the exporter
var exporter = OtlpGrpcExporter.to("collector", 4317);
var sub = Pipeline.from(receiver.traces())
        .owns(exporter)              // mandatory, or a Cleaner scolds you at GC
        .to(exporter.traces());

// After: the facet carries the exporter's lifecycle; nothing to remember
var exporter = OtlpGrpcExporter.to("collector", 4317);
var sub = Pipeline.from(receiver.traces())
        .to(exporter.traces());
```

Deletes the `to(terminal, owner)` overload, the Auto/Explicit/Hidden taxonomy, the shutdown chapter, and the cheat-sheet. The `Cleaner` warning survives as a true backstop rather than the primary safety net. `owns()` survives only as an escape hatch for a resource genuinely hidden behind an opaque lambda (for example a count connector's separate downstream exporter). The one behavioral change (closing one facet drains the whole shared channel) is exactly what a gateway wants at teardown and is one sentence to document.

*Debate.* No dissent. The pragmatist confirmed it is accidental, not inherent (the Collector avoids it by never inferring lifecycle from data flow). This also resolves the `OtlpExporter`-is-not-an-`Exporter<T>` naming collision (see F8): each facet becomes a real single-signal exporter, so the facade is a coherent bundle of four exporters sharing a channel.

#### F2. Make retryability explicit in `ConsumeResult.Rejected` `[High]`

Today a whole-batch rejection's retry semantics are encoded by whether `cause` is null:

```java
// model/ConsumeResult.java + codec/DeliveryResults.java
ConsumeResult.retryableRejected("queue full");        // cause == null -> gRPC UNAVAILABLE / HTTP 503
ConsumeResult.permanentRejected("bad batch", cause);  // cause != null -> gRPC INTERNAL   / HTTP 500
```

This conflates two orthogonal axes: *do I have a diagnostic cause?* and *should the sender retry?*

- A genuinely transient failure that happens to carry an exception (a downstream is briefly down, surfacing an `IOException`) is mis-mapped to **permanent** INTERNAL/500. In a data plane that means silent, unretried data loss: a correctness bug, not just an ergonomics wart.
- A permanent policy or validation rejection has no exception, so it cannot be expressed without synthesizing a fake throwable. `permanentRejected` *requires* a non-null cause.

**Fix.** Carry retryability explicitly and let `cause` be pure diagnostics on either disposition. This mirrors the Collector's `consumererror.NewPermanent(err)`, where a transient error legitimately also carries a cause.

```java
// After: retryability is stated, cause is optional and orthogonal
ConsumeResult.retryable("queue full");                  // transient, no cause
ConsumeResult.retryable("downstream down", ioException);// transient WITH a diagnostic cause
ConsumeResult.permanent("policy rejected batch");       // permanent WITHOUT a synthetic throwable
```

Behavior-compatible at the wire (`SignalResponses` still maps `Partial` to `partial_success`); it stops overloading a nullable field and removes the "intentionally nullable spots" caveat from the docs. Either a `boolean retryable` flag or a `Transient`/`Permanent` subtype split works; the subtype split pattern-matches more cleanly alongside `Accepted`/`Partial`.

*Debate.* Consensus. The familiarity advocate notes this makes the type *more* familiar, not less: `CompletableResultCode` has no such trap.

### Tier 2: Familiarity alignment (export half; keep typed config)

The export builders are where an opentelemetry-java user has the deepest muscle memory, and where otlp4j diverges most cheaply-fixably. There are currently **no** `set`-prefixed setters anywhere in the public surface.

#### F3. Adopt the universal `set*` builder prefix `[High]`

Rename every public builder setter to `set*` across `ClientConfig.Builder`, `ServerConfig.Builder`, and the four transport builders: `endpoint`→`setEndpoint`, `timeout`→`setTimeout`, `compression`→`setCompression`, `retry`→`setRetryPolicy`, `tls`→`setTls`, and so on. The `set` prefix is the recognition signal of an OTel exporter builder; its absence is felt on the very first chained call. This is a near-mechanical rename with no semantic change and the highest familiarity-gain per unit of churn.

#### F4. Add `setEndpoint(String url)` `[Med]`

Accept a single URL string parsing `scheme://host:port[/prefix]`, and demote `endpoint(host, int)`. One URL string is the dominant OTel call shape and matches `OTEL_EXPORTER_OTLP_ENDPOINT` verbatim. The per-signal path suffix (`/v1/traces`) stays the transport's job, because the multi-signal facade correctly takes a base authority rather than a per-signal URL (see the keep list on the facade).

#### F5. Collapse the header trio to `addHeader` + `setHeaders` `[Med]`

`ClientConfig.Builder` exposes three header methods: `header(k, v)`, `headers(Map)` (replace), and `addHeaders(Map)` (merge). This is both unfamiliar and redundant. Match the SDK's two-method shape: `addHeader(k, v)` to add and `setHeaders(...)` to replace.

#### F6. Give `RetryPolicy` a builder and the missing `backoffMultiplier` `[Med]`

`RetryPolicy` is the one config type opentelemetry-java users already know by name, but otlp4j's mismatches it twice: a positional factory `exponential(maxAttempts, initialBackoff, maxBackoff)` instead of the named-setter builder they expect, and no `backoffMultiplier` field at all. Add a `RetryPolicy.builder().setMaxAttempts(...).setInitialBackoff(...).setMaxBackoff(...).setBackoffMultiplier(...)` and let users paste their SDK retry config across.

#### F7. Keep typed config; add familiar front doors `[Low]`

Do **not** regress to the SDK's stringly/`byte[]` config. otlp4j's `Tls` sealed type (`Disabled`/`SystemTrust`/`Custom`) and `Compression` enum are genuinely cleaner than `setTrustedCertificates(byte[])` and `setCompression("gzip")`, and the sealed `Tls.custom(cert, key, trust)` expresses server identity and mTLS that the SDK's client-only `byte[]` API cannot. Keep them as the real API; optionally add delegating front doors (`setCompression(String)`, `setTrustedCertificates(byte[])`, `setSslContext(...)`) for reflexive familiarity. Add the cheap additive niceties too: a no-arg `shutdown()`/`forceFlush()` alongside the `Duration` overloads, plus `getDefault()` and `toBuilder()`.

Combined before/after for the export builder:

```java
// Before
OtlpGrpcExporter.builder()
        .endpoint("collector.example.com", 4317)
        .timeout(Duration.ofSeconds(5))
        .compression(Compression.GZIP)
        .header("authorization", "Bearer " + token)
        .retry(RetryPolicy.exponential(5, Duration.ofSeconds(1), Duration.ofSeconds(30)))
        .build();

// After: set* prefix, URL endpoint, addHeader, RetryPolicy builder; typed config retained
OtlpGrpcExporter.builder()
        .setEndpoint("https://collector.example.com:4317")
        .setTimeout(Duration.ofSeconds(5))
        .setCompression(Compression.GZIP)
        .addHeader("authorization", "Bearer " + token)
        .setRetryPolicy(RetryPolicy.builder()
                .setMaxAttempts(5)
                .setInitialBackoff(Duration.ofSeconds(1))
                .setMaxBackoff(Duration.ofSeconds(30))
                .build())
        .build();
```

*Debate.* The familiarity advocate wanted to push further (stringly compression, byte[] TLS, `export`/`CompletableResultCode`); the pragmatist vetoed each as a regression. F3 to F7 is the converged core: adopt OTel's *idioms* (set-prefix, URL, header shape, RetryPolicy builder) while keeping otlp4j's *better-typed* config.

### Tier 3: Simplicity and surface reduction

otlp4j ships ~35 user-facing top-level types across 8 packages plus a second `spi` module to do a job opentelemetry-java models with three methods and a result code. The gateway legitimately needs more, but not this much more.

#### F8. Cut or fold the orphaned single-signal `Exporter<T>` `[Med]`

`exporter/Exporter<T>` (a `Sink<T>` + lifecycle, with default no-op shutdown/forceFlush) is referenced **only by a test** (`ExporterDefaultsTest`); no built-in implements it. It also collides in name with `OtlpExporter`, which is a different shape entirely (a facade, not a `Sink`). Either cut it (a custom terminal implements `Sink<T>` plus the lifecycle interface), or let F1 subsume it, where each lifecycle-bearing facet *becomes* a coherent single-signal exporter. Either way the "single-vs-multi-signal terminal" duality and the name collision both disappear.

#### F9. Merge `Drainable` + `ForceFlushable` into one `Lifecycle` `[Med]`

Two lifecycle interfaces where `forceFlush` is a no-op on everything except `BatchingProcessor`. Collapse to one:

```java
interface Lifecycle extends AutoCloseable {
    CompletionStage<Void> shutdown(Duration timeout);
    default CompletionStage<Void> forceFlush(Duration timeout) { /* no-op */ }
    default void close() { shutdown(Duration.ofSeconds(10)).toCompletableFuture().join(); }
}
```

*Debate resolved.* The minimalist wanted both interfaces gone; the pragmatist vetoed losing the *operation*, since flush-without-teardown and drain-and-stop are distinct operator actions present in both the SDK and Collector. Synthesis: keep both **operations**, but as one **concept**, with `forceFlush` as a defaulted method rather than a standalone marker interface (the SDK never made `Flushable` separate either).

#### F10. Dissolve the `core` junk-drawer package `[Med]`

`dev.nthings.otlp4j.core` holds 12 unrelated public types: `Sink`, the four signal SAMs, `Source`, `Drainable`, `ForceFlushable`, `OverflowPolicy`, `PipelineHandle`, `Telemetry`, `ThrowingConsumer`. That is not a concept, it is a bucket. Redistribute to `pipeline` (`Source`, `PipelineHandle`, `OverflowPolicy`), `exporter`/`receiver`, and a small lifecycle home, and retire the `core` name. Cuts one package from the user's mental map.

#### F11. Pick one fan-out spelling `[Low]`

`branch().fanOut().join()`, the public `FanOut.of(...)`, and the `to(terminal, owner)` overload are three spellings of "send to N peers and attach a lifecycle." `BranchImpl.join()` literally calls `stage.to(FanOut.of(peers))`. After F1 removes the `owns`/owner machinery, keep a single public fan-out form (most likely `FanOut` as a reusable `Sink`, with the branch DSL as optional sugar or removed).

#### F12. Keep the four signal SAMs, consolidate their factories `[Low]`

`TraceSink`/`MetricSink`/`LogSink`/`ProfileSink` are worth keeping for lambda ergonomics and rough SDK familiarity (one type per signal), but each carries its own copy of the `accepting`/`fromStage` static factories. Keep the four types as the ceiling; move the duplicated factories onto `Sink`. (See F14 for their names.)

### Tier 4: Naming consistency and extension points

#### F13. Rename the second SPI layer `[Med]`

There are two packages named `spi`, at different abstraction levels, and the collision is a real discoverability hazard:

- `dev.nthings.otlp4j.spi` (in `otlp4j-api`) is the **wire contract you implement** to add a protocol: `OtlpClient`, `OtlpServer`, `Dispatchers`.
- `dev.nthings.otlp4j.transport.spi` (the separate `otlp4j-transport-spi` module) is the **shared composition kit** that adapts those contracts into an `OtlpExporter`/`Receiver`: `ClientExporter`, `ServerReceiver`, `SignalSource`, `ReceiverTap`, `MulticastPublisher`. It is reused verbatim by both the gRPC and HTTP transports.

Keep both (they are distinct levels, not redundancy), but rename the second to `transport.support` (or `transport.kit`) so it stops twinning with the protocol `spi`. Only the first is an extension point a user implements.

*Debate.* The minimalist proposed deleting one SPI; the pragmatist vetoed (both are real) and both agreed the *names* are the problem.

#### F14. Fix the singular/plural sink mismatch `[Low]`

`TraceSink` is singular while everything around it is plural: `TracesData`, `receiver.traces()`, `onTraces`, `BatchingProcessor.forTraces`, and the Collector's plural `consumer.Traces`. Rename `TraceSink`→`TracesSink`, `MetricSink`→`MetricsSink`, `LogSink`→`LogsSink`, `ProfileSink`→`ProfilesSink`. Keep the item-level connector names `spanCount`/`logRecordCount` unchanged: they count *items* (`Span`, `LogRecord`), which is correct and already OTel-idiomatic.

Do **not** rename the batch nouns to `Span`/`LogRecord`. `TracesData`/`MetricsData`/`LogsData`/`ProfilesData` mirror the OTLP proto `*Data` messages and the Collector's pdata; a `Span` is an item, not a batch. A `TracesSink` carries a `TracesData` tree (ResourceSpans → ScopeSpans → Span), not a `Collection<SpanData>`, so the plural name is the more honest one.

#### F15. Promote `Processor` and `Connector` to first-class contracts `[Low]`

The gateway's two most Collector-resonant extension points are informal. A processor is "just a `Sink<T>` wrapping a downstream `Sink<T>`" with no named contract, and `CountConnector` is a package-private `final class`. Expose named `Processor` and `Connector` interfaces so these read as components (as in the Collector) rather than as a convention plus a static helper.

#### F16. Consider an `otlp4j-extras` module (flagged, not forced) `[Open]`

The opinionated batteries (`Transforms`, the two `Connectors`, `TelemetryTap`) inflate the core surface a newcomer reads. Moving them to a clearly-named optional module keeps the receive → transform → export spine small while still shipping them. Keep `BatchingProcessor` in core: batching is the gateway's reason to exist. This trades discoverability for a leaner core; see Open decisions.

## What to deliberately keep

So the cleanup does not amputate capability, these are domain-correct and should survive, several of them precisely *because* the SDK lacks them:

| Abstraction | Why it stays |
| --- | --- |
| `Receiver` + `Source` | The SDK never receives. Without an ingest endpoint there is no gateway. This is the most SDK-foreign, most load-bearing half of the library. |
| `ConsumeResult` Accepted/Partial/Rejected | The receiver must echo OTLP `partial_success`. `SignalResponses` serializes `Partial` to `setRejectedSpans`/`setRejectedDataPoints`/etc. `CompletableResultCode` is boolean and cannot carry this. |
| The `consume` verb | A position-neutral request/ack used receive-side, mid-pipeline, and at the terminal alike. It is the Collector verb; `export` only reads right at the very end (and the wire SPI already says `exportTraces`). |
| `Pipeline` / `branch` / `fanOut` | Routing and tee-ing are core gateway jobs. Fan-out shares one immutable batch across peers and merges acks worst-case-not-sum (`ConsumeResult.fanOutMerge`), subtle and correct, exactly what you don't want every user to reinvent. |
| `Connectors` | Signal-to-signal derivation (traces→metrics, logs→metrics) is a real Collector component (the `count` connector), borrowed straight across. |
| `TelemetryTap` | Live observation with genuine isolation from the OTLP ack path: a lagging tap subscriber never back-pressures the sender. A real operability win. |
| Multi-signal facade exporter | A gateway forwards mixed batches over one shared channel; one `OtlpExporter` drives all four signals via one `OtlpClient`. The SDK ships one class per signal because it emits one signal per pipeline; four independent classes here would force users to assemble shared-channel plumbing. Keep the facade; F1 makes its facets coherent. |

The recommendations above never cut anything in this table; F1 and F8 *strengthen* the facade rather than splitting it.

## Phased roadmap

1. **Quick familiarity pass (mechanical, low risk).** F3 (`set*` prefix), F5 (header methods), F6 (RetryPolicy builder), F4 (`setEndpoint(url)`), and the additive F7 front doors. No behavior changes; immediate recognition win.
2. **Footguns and correctness (behavioral, highest value).** F1 (facet lifecycle, deletes the ownership taxonomy) and F2 (explicit retryability, fixes the transient-with-cause mis-map). These two matter most and should land together with their doc simplification.
3. **Surface reduction.** F8 (orphan `Exporter<T>`), F9 (one `Lifecycle`), F10 (dissolve `core`), F11 (one fan-out spelling), F12 (consolidate SAM factories).
4. **Naming and extension points.** F13 (`transport.spi` rename), F14 (plural sinks), F15 (`Processor`/`Connector` contracts); decide F16.

Sequencing rationale: do the mechanical renames first so later behavioral PRs aren't tangled with cosmetic churn; land F1+F2 before the 1.0 freeze because they change observable contracts; treat Tier 4 as polish that can trail.

## Open decisions

A few genuine forks are left to the maintainer:

- **F16: extras module vs batteries-in-core.** Leaner core surface and clearer "what's essential" versus one-dependency convenience and discoverability. Recommendation leans toward extracting `Transforms`/`Connectors`/`TelemetryTap` while keeping `BatchingProcessor` core, but it is a values call.
- **F2 shape: `boolean retryable` field vs `Transient`/`Permanent` subtypes.** The subtype split pattern-matches more cleanly next to `Accepted`/`Partial`; the boolean is a smaller change.
- **F11: which fan-out spelling survives** (`FanOut.of` as a reusable sink, or the `branch().join()` DSL as sugar). Both can't stay without re-introducing redundancy.
- **F14 reach: stop at plural sinks, or also align the per-signal source/dispatcher names** for full consistency.

## Appendix

### A. Per-lens sub-axis scorecards

**Familiarity to opentelemetry-java (overall C-, ≈2.2/5)**

| Axis | Score | Note |
| --- | --- | --- |
| Naming | 2 | Right stems (`OtlpGrpcExporter`), but novel `Sink`/`Source`/`Receiver`/`tap`/`connector` vocabulary and singular `TraceSink`. |
| Builder idiom | 2 | No `set` prefix anywhere; no `getDefault()`/`toBuilder()`. |
| Result / async | 2 | `consume`→`CompletionStage<ConsumeResult>` vs `export`→`CompletableResultCode` (domain-justified, see keep list). |
| Config types | 3 | Concepts align; shapes diverge but are arguably better-typed. |
| Entry-point shape | 2 | Multi-signal facade vs one-class-per-signal (domain-justified); lifecycle footgun has no SDK precedent. |

**Simplicity / conceptual weight (overall D+, ≈1.6/5)**

| Axis | Score | Note |
| --- | --- | --- |
| Surface parsimony | 2 | ~35 top-level types beyond the model; `core` holds 12. |
| Orthogonality | 2 | `Sink`/`Exporter<T>`/`OtlpExporter` overlap; `Drainable` vs `ForceFlushable`; three fan-out spellings. |
| Footgun-resistance | 1 | Facet ownership; retry-by-null-cause; `forceFlush` a documented no-op; `forProfilesUnsafe()`. |
| Learnability | 2 | The first non-trivial program forces `Sink`+`Source`+`Pipeline`+`owns` before telemetry moves. |
| Documentation burden | 1 | A shutdown chapter plus an ownership cheat-sheet to neutralize one design choice. |

**Domain modeling (overall B+, ≈4.0/5)**

| Axis | Score | Note |
| --- | --- | --- |
| Domain fit | 5 | Abstraction set maps almost 1:1 onto Collector components; nothing essential missing or fictional. |
| Internal consistency | 3 | Real seams: `OtlpExporter` not an `Exporter<T>`; singular/plural sinks; twin `spi`; retry-by-nullness. |
| Collector alignment | 4 | `consume`/`ConsumeResult`/`Receiver`/`Connector`/`Pipeline` all Collector idioms; two naming misses. |
| Necessary-complexity honesty | 4 | Most complexity is earned (async, partial-success, fan-out merge); the accidental parts are F1/F2/F13. |
| Extensibility | 4 | Custom transport is well-factored; custom processor/connector works but is informal (F15). |

### B. Divergence ledger (export half vs opentelemetry-java)

| otlp4j current | opentelemetry-java | Recommendation | Finding |
| --- | --- | --- | --- |
| `endpoint(host, int)` + `path(String)` | `setEndpoint(String url)` | Add `setEndpoint(url)`, demote host/port | F4 |
| `timeout(Duration)` (no `set`) | `setTimeout(Duration)` / `(long, TimeUnit)` | `setTimeout(...)`; optional TimeUnit overload | F3 |
| `compression(Compression)` | `setCompression(String)` | `setCompression(...)`, keep enum, add String door | F3, F7 |
| `header` / `headers` / `addHeaders` | `addHeader` + `setHeaders` | Collapse to two | F5 |
| `tls(Tls)` sealed | `setTrustedCertificates(byte[])` / `setClientTls` / `setSslContext` | Keep `Tls`, add familiar doors | F7 |
| `RetryPolicy.exponential(a, b, c)` factory | `RetryPolicy.builder().set*` | Add builder + `backoffMultiplier` | F6 |
| `Drainable.shutdown(Duration)` only | `shutdown()` no-arg | Add no-arg overloads | F7, F9 |
| No `getDefault()` / `toBuilder()` | both present | Add | F7 |
| `OtlpExporter` facade with facets | one class per signal+transport | Keep facade (domain), align facets | keep list, F1 |
| `consume` / `ConsumeResult` | `export` / `CompletableResultCode` | Keep (carries partial-success) | keep list, F2 |

### C. Inventory pointer

The full public-surface inventory (modules, packages, every public type by role, builder method lists) was produced during review and matches `docs/public-api.md`. The change sites cited above:

- `otlp4j-transport-spi/.../transport/spi/ClientExporter.java`: facet lambdas, leak `Cleaner` (F1)
- `otlp4j-api/.../pipeline/Pipeline.java`, `pipeline/PipelineLifecycle.java`: `owns`/`to`, auto-collect (F1, F11)
- `otlp4j-model/.../model/ConsumeResult.java`, `otlp4j-codec/.../codec/DeliveryResults.java`, `codec/SignalResponses.java`: result semantics (F2)
- `otlp4j-api/.../config/ClientConfig.java`, `config/RetryPolicy.java`, `config/Tls.java`: builders and config (F3 to F7)
- `otlp4j-api/.../exporter/OtlpExporter.java`, `exporter/Exporter.java`: facade and orphan (F1, F8)
- `otlp4j-api/.../core/*`, `otlp4j-api/.../module-info.java`: `Lifecycle`, `core` package, sink names (F9, F10, F12, F14)
- `otlp4j-transport-spi/.../module-info.java`, `connector/Connectors.java`, `connector/CountConnector.java`: SPI rename, connector contract (F13, F15)
