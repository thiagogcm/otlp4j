# otlp4j — Road to 1.0

This plan defines the work required to cut a stable `1.0`. It is organized around one question: **what must be right before the surface freezes.**

## Freeze charter

otlp4j is pre-1.0 and carries no compatibility promise yet, so breaking changes are free today and expensive forever after the freeze. The whole plan follows from that asymmetry:

- **Spend the entire breaking-change budget before 1.0.** Every change that should ever break the public surface — a removed type, a renamed member, a changed contract — lands now or never.
- **Ship only what must be correct at 1.0.** Anything that can be *added* later without breaking callers is out of scope and revisited from real usage feedback.
- **JDK 25 is the baseline.** Raising or lowering it after 1.0 is a major-version event.

This document is the decision record and the freeze gate. It is not the public API documentation: the user-facing docs (`README.md`, `docs/public-api.md`, package javadoc) describe only the target surface by its final names — they never narrate a change or carry "formerly" wording.

### How to read it

- **§1 Compatibility policy** — what "1.0 stable" promises. Blocking.
- **§2 Breaking changes** — surface-defining work; every item is **Decided**.
- **§3 Correctness & ergonomics** — non-breaking, so not a compatibility blocker, but worth doing for a credible 1.0.
- **§4 Surface hygiene** — smaller pre-1.0 cleanups.
- **§5 Frozen surface** — the tiered inventory the freeze is checked against.
- **§6 Out of 1.0 scope** — deferred, additive, feedback-driven.
- **§7 Sequencing** — the order to land §2/§4 without re-churning files.
- **§8 Definition of done** — the auditable freeze gate.
- **§9 Decisions log** — the previously-open calls, now settled, consolidated.

Every decision in §2–§4 is settled; §9 is the consolidated log of how the contested ones landed.

---

## 1. Stability and compatibility policy (blocking)

The policy must be written down (`docs/compatibility.md`, linked from the README) and adopted before the freeze. Without it, "1.0" promises nothing specific.

**What counts as public API.** A type is part of the promise **only if it is `public` *and* lives in an unqualified `exports` package of a published module.** `public` alone is not enough. This rule puts the qualified-exported `codec` and generated proto packages outside the promise by construction, and makes "move a type to a non-exported package" the canonical way to remove something from the surface.

**Covered (the Application API), per current exports:**

- `dev.nthings.otlp4j.model`
- `dev.nthings.otlp4j.{core, pipeline, receiver, exporter, processor, connector, config}`
- `dev.nthings.otlp4j.{transport.grpc, transport.http}`

Covered changes guarantee **source and binary compatibility** across all of 1.x: no removed or renamed exported types/members, no narrowed returns or widened checked throws, and frozen permit lists on **every** sealed type (load-bearing for exhaustive switches — `ConsumeResult`, `Metric.Data`, `Telemetry`, `AttributeValue`, `Tls`, `Pipeline.Stage`/`Branch`, and the rest).

**Excluded, even though shipped:**

- Any `*.internal` package and the qualified-exported `codec` / proto packages.
- Anything annotated `@Experimental` (today, profiles).
- The `otlp4j-proto` / `otlp4j-codec` artifacts as a *compile* surface (runtime carriers only).

**Defaults — two kinds, governed differently:**

- **Tuning defaults** (flush cadence, buffer sizes, default executors) are not contract and may change in a minor.
- **Contract / safety defaults** (the acknowledgement a receiver gives an unattached signal; the interface a receiver binds by default) *are* part of the promise and are frozen at 1.0. §2.4 settles these while they are still free to change.

**Two tiers, because they evolve under different rules:**

| Tier                                                        | Audience          | Evolution rule                                                                                                                                                                                                                                                                                                                      |
| ----------------------------------------------------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Application API**                                         | callers           | Grows freely; callers do not implement most of these. The *implemented* surface — the functional SAMs (`TraceSink`/`MetricSink`/`LogSink`/`ProfileSink`, `Transform`, `ThrowingConsumer`; kept single-method) and the extension interfaces `Sink<T>` and `Exporter<T>` — grows by `default` methods only, the same rule as the SPI. |
| **Provider SPI** (`spi`, plus the internal transport bases) | transport authors | Implemented types. Grow **only** via `default` methods. A new OTLP signal is a major-version event by design (it touches `Dispatchers`, `OtlpClient`, `OtlpServer`, and the facets together). State this in `spi/package-info.java`.                                                                                                |

**`@Experimental` is the stability boundary.** Absence of the annotation means stable. Keep its `CLASS` retention (visible in bytecode/javadoc/IDE, no runtime cost). Add a paired `@Internal` marker so "no annotation = stable" is enforceable for types that must be `public` for cross-module reasons but are not for end users. Write the disclaimer into the policy: *"`@Experimental` elements are excluded from the compatibility promise and may change or be removed in any release."*

**Profiles stay experimental under a stable core — the design already supports it.** `ProfilesData` models only the stable resource/scope wrapper and carries the `v1development` payload as opaque bytes, so the unstable schema never enters the typed model. Traces, metrics, and logs ride stable OTLP `v1`; profiles is quarantined behind opacity plus the annotation. The follow-through is completing annotation coverage (§4).

---

## 2. Breaking changes (the 1.0-blocking set)

### 2.1 Remove implementation detail from the public surface

**`internalize-signal-and-mergers` — Decided.** `processor.Signal` and `processor.BatchMergers` are public but are internal multiplexers for batching/dispatch (`Signal`'s own javadoc says so; `BatchMergers` has no external consumers). Neither appears in the public type table; no other module uses them.

- [ ] Move both to a non-exported `dev.nthings.otlp4j.internal` (or `processor.internal`) package.
- [ ] Keep `BatchingProcessor` reaching them internally; drop them from `exports processor`.
- [ ] Update the in-module `SignalTest` / `BatchMergersTest` imports.

**`internalize-transport-bases` — Decided (internal).** `AbstractOtlpExporter` and `AbstractOtlpReceiver` are `public abstract` in application packages but exist only so the bundled transports can subclass them. Leaving them as unqualified-exported public types silently promises subclassing stability to every application.

- [ ] Move both to `dev.nthings.otlp4j.spi.internal`, qualified-exported only to `transport.grpc` and `transport.http`. The supported extension points are the `spi` contracts plus `Exporter<T>`, not these bases.
- [ ] Both transports compile against the new location; the qualified export names both modules.
- [ ] They are absent from the public type inventory.

**`settle-exporter-extension-point` — Decided (keep + document).** `exporter.Exporter<T>` is a public extension interface (`Sink` + `Drainable` + `ForceFlushable`) implemented by nothing in production today, but with the transport bases going internal it becomes the natural public way to write a custom terminal.

- [ ] Keep `Exporter<T>` as *the* supported way to build a custom typed terminal (facets + the lifecycle it inherits), and document it as such — distinct from wiring a bundled exporter. Govern it by the default-only growth rule (§1). Keeping it is non-breaking, so this is a documentation task, not a code change.

### 2.2 Naming — clarity and JDK-collision avoidance

All renames below are source-breaking and therefore pre-1.0-only.

| Target name           | Type                                                                                   | Why                                                                                                                                                                                                              |
| --------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `model.TracesData`    | the trace batch                                                                        | The lone singular among `MetricsData`/`LogsData`/`ProfilesData`; OTLP itself names the message `TracesData`. `spans()` / `spanCount()` accessors stay.                                                           |
| `core.PipelineHandle` | the wired-pipeline handle (returned by `Pipeline.to(...)` and `Source.subscribe(...)`) | Removes the clash with `java.util.concurrent.Flow.Subscription`, which users meet in the tap a few lines away, and reads well at the dominant `Pipeline.to` call site.                                           |
| `core.ForceFlushable` | the force-flush capability                                                             | Unambiguous against `java.io.Flushable` (different signature) for users who import both, and matches the `forceFlush` method. Stays separate from `Drainable` (`Receiver` is drainable but not force-flushable). |

- [ ] `TracesData`: rename across model, api, codec, both transports, testing, samples, tests, and `///` javadoc links.
- [ ] `PipelineHandle`: **semantic (IDE) rename only** — `Flow.Subscription`/`Flow.Subscriber` appear in the same files (tap code, tests), so a textual sweep would rewrite the JDK type.
- [ ] `ForceFlushable`: textual rename is safe today (`java.io.Flushable` is not used in the tree), but prefer the IDE rename for the `///` links.

**`batcher-knob-names` — Decided.** On `BatchingProcessor` both builder parameters are named `items` yet both count *batches*: `maxBatchSize` gates on `queue.size()`, and `queueCapacity` sizes the batch queue. Rename for honesty.

- [ ] `maxBatchSize` → `flushThreshold(int batches)` — chosen over `maxQueuedBatches`, which would read as a synonym of the hard cap; `flushThreshold` (drain trigger) and `queueCapacity` (hard cap) name two genuinely different knobs.
- [ ] `queueCapacity`'s parameter is renamed to `batches` too — fix both mislabels, not one.
- [ ] `docs/public-api.md` describes both knobs in batch units.

**`unify-overflow-policy` — Decided (merge with site-neutral names).** `processor.DropPolicy` and `receiver.BackpressureStrategy` share constant *spellings* but **not semantics**: the terminal constant means "reject this batch (retryable)" for the processor and "terminate the subscription with an error" for the tap, and `BLOCK` blocks different threads at each site.

- [ ] Merge into one `core.OverflowPolicy` with **site-neutral** constants `{DROP_OLDEST, DROP_NEWEST, BLOCK, FAIL}` — one vocabulary; `FAIL` reads as "fail the operation" for both the rejected batch (processor) and the terminated stream (tap), avoiding the misleading `ERROR`/`REJECT` spellings.
- [ ] Document the per-site effect of each constant (which thread `BLOCK` blocks; batch-reject vs. stream-terminate for `FAIL`).

### 2.3 Remove redundant mechanisms

**`remove-pipeline-peek` — Decided (with honest migration framing).** `Pipeline.peek` is a third observation mechanism that runs its observer inline-before-the-terminal and silently swallows failures. It should go — and removal is the reversible choice (re-adding a method post-1.0 is additive; keeping then removing it would be breaking). There is **no behavior-preserving one-liner replacement**, and the docs must say so:

- [ ] Remove `Pipeline.peek`.
- [ ] Document the trade-offs of each in-path alternative honestly: an in-path observing `Sink` (e.g. via `fanOut`) runs *concurrently* with the terminal and a rejecting observer rejects the batch back to the sender; an identity `transform` runs inline but turns an observer throw into a `permanentRejected` unless it catches its own exceptions. The maintainer decides whether "an observer fault rejects telemetry" is acceptable guidance.
- [ ] Sweep all `peek` references in prose: `docs/public-api.md` (2 spots), `docs/architecture.md`, and `pipeline/package-info.java`.

**`trim-consumeresult-factories` — Decided.** `ConsumeResult` ships `retryableRejected`, `permanentRejected`, `rejected(String)`, and `rejected(String, cause)` — four ways to reject. `retryableRejected` / `permanentRejected` are the blessed pair.

- [ ] Cut `rejected(String)`. Keep at most one low-level escape hatch (`rejected(String, @Nullable cause)`) for transports forwarding a cause decided elsewhere, and document it as such.
- [ ] Update the test files that call `rejected(String)` (≈6).

**`hide-batcher-concurrency-knobs` — Decided.** `BatchingProcessor.Builder` exposes JDK concurrency primitives directly: `dropCounter(LongAdder)` and `scheduler(ScheduledExecutorService)`.

- [ ] Remove `dropCounter(LongAdder)` — `droppedCount()` already exposes the number.
- [ ] Remove `scheduler(ScheduledExecutorService)` from the public builder; use an internal default. It is additive to re-introduce later if feedback wants a custom scheduler.

### 2.4 Contract and safety defaults

These are frozen-contract defaults (§1), so they are settled now while they are still free to change. Both flips preserve the prior behavior as an explicit opt-in — only the default moves.

**`unattached-source-default` — Decided (reject-retryable).** An unattached signal source today acknowledges the batch as **accepted** and then drops it — for a data plane, "tell the sender it is safe to forget this, then discard it." It bites two ways: a gateway that attaches only `traces()` silently black-holes the other three signals while reporting success, and a receiver wired *after* `start()` loses everything in the gap.

- [ ] Default an unattached source to `retryableRejected` (→ gRPC `UNAVAILABLE` / HTTP 503), so a missing or mis-ordered attachment is loud and retried, not silently lost.
- [ ] Accept-and-drop remains available as an explicit per-signal `discard(...)` opt-in.

**`receiver-bind-default` — Decided (loopback).** The receiver today defaults to plaintext on the wildcard interface (`0.0.0.0`), so the easy path exposes an unauthenticated decoder on every interface.

- [ ] Default `bindHost` to loopback; binding the wildcard interface becomes an explicit opt-in. Flips the unsafe default without removing capability.

---

## 3. Correctness and ergonomics (non-breaking; recommended for 1.0)

**`exporter-ownership-footgun` — Decided (loud-leak for 1.0; auto-drain deferred).** An exporter's signal facets are plain sinks with no lifecycle, so attaching `exporter.traces()` to a pipeline wires delivery but not teardown: forget `owns(exporter)` and the client channel leaks silently. Even the canonical sample wears a belt and suspenders (`owns(exporter)` *plus* a `finally` close), and a 7-row "lifecycle cheat sheet" exists to compensate.

Two facts shape the fix:

- It is **not a breaking change** to address: a facet can implement `Drainable`/`ForceFlushable` while still returning its `TraceSink` static type, so callers compile unchanged. By the freeze charter, that makes the deeper fix *additive* — it can ship in a 1.x minor and is not a freeze blocker.
- **Naive auto-drain is unsafe for the mainline topology.** One exporter serves all four signals, and each signal is its own `Pipeline.from(source)` subscription. If every attached facet auto-registers the exporter for teardown, the *first* subscription to shut down closes the shared channel and the other three suffer use-after-close (and lose their final drained batch). Close-once memoization does not fix this — it dedupes a double close, not a *premature* one.

Resolution:

- [ ] **For 1.0 (non-breaking, cheap, safe):** make the leak *loud* — a `Cleaner`-based warning when an exporter is garbage-collected without having been shut down — and make the explicit-ownership docs/examples first-class (safe build → wire → `start()`; one canonical fan-out-with-per-destination-queue topology).
- [ ] **Deferred to 1.x (§6):** facets carry lifecycle with **ref-counted teardown** — the exporter closes only after the last registration releases, explicitly *not* memoization. It is additive, so it need not gate the freeze, and its cross-subscription quiescing + first-caller-deadline design wants soak time it would not get late in the cut. Count connectors must then forward their downstream facet's lifecycle so the exporter behind a count sink is reachable too.

The loud-leak mitigation is sufficient for the freeze; the redesign is a 1.x item.

---

## 4. Surface hygiene (pre-1.0, mostly mechanical)

- **`transforms-discoverability` — Decided (keep the split, cross-link).** `Transform` (the DSL primitive `Pipeline.transform` consumes) stays in `pipeline`; `Transforms` (the ready-made processor-tier transforms) stays in `processor` beside the other in-pipeline building blocks — consistent with the Collector taxonomy the library targets (see *keep-with-rationale* below). Fix discoverability with a cross-link from `pipeline/package-info.java` and `docs/public-api.md`, not a move that fights the taxonomy.
- **`document-transforms-map` — Decided.** `Transforms` publicly ships `mapSpans`, `mapLogRecords`, and `mapTracesResource`/`mapMetricsResource`/`mapLogsResource`/`mapProfilesResource` (used in the sample) but the docs describe only filters and attribute-setters. Document them as API — do not freeze undocumented public methods.
- **`complete-experimental-coverage` — Decided.** Annotate **every public element whose signature names a profiles type** `@Experimental`, not just `ProfilesData` — including `Receiver.profiles()`, `TelemetryTap.profiles()`, the exporter `profiles()` facet, `BatchingProcessor.forProfilesUnsafe()`, `core.ProfileSink`, `core.Telemetry.Profiles`, `Transforms.mapProfilesResource` / `withProfilesResourceAttribute`, and the profiles slot of `spi.Dispatchers`. Drive completeness from the surface manifest (§5) rather than a hand list. Add the `@Internal` marker from §1.
- **`packaging-gate` — Decided.** Set `maven.deploy.skip` on `otlp4j-samples`. Modularize `otlp4j-testing` (give it a `module-info.java`) and treat it as a supported test-support artifact — the docs already point users at `testing.FlowSubscribers`, so removing it would strand documented usage. Add a release check asserting the artifact ↔ module-name mapping; verify/bump the reproducible-build timestamp and release `revision`.
- **`spi-dispatchers-policy` — Decided (keep + state policy).** `spi.Dispatchers` is a four-component record, so a fifth OTLP signal is a breaking change to a "stable" type. Keep the record; the SPI policy states that adding an OTLP signal is a major-version event (it co-changes `Dispatchers`, `OtlpClient`, `OtlpServer`, and the facets together). A signal-keyed builder would be over-engineering for 1.0.
- **`keep-with-rationale` — Decided.** Record these so they are not "cleaned up" later:
  - `ConsumeResult<T>`'s type parameter is an intentional phantom — it enforces signal discipline on `Sink<T>`/`Source<T>` and prevents cross-signal rejection mixing; the internal `retag` casts are the deliberate cost.
  - The `processor` / `connector` split mirrors the OTLP Collector taxonomy (processors are in-pipeline single-signal transforms/buffers; connectors do cross-signal derivation). Keep it; state the distinction in `docs/architecture.md`.

---

## 5. Frozen surface inventory (blocking deliverable)

The authoritative artifact is a generated, checked-in manifest of every exported package → public type → public member, tagged by tier, with a CI diff gate (revapi/japicmp) that fails on any unreviewed change after the freeze. The table below is the *illustrative* intent the manifest is checked against — the manifest, not this table, is authoritative.

| Tier                      | Contents (representative)                                                                                                                                                                                                                                                                           |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Tier 1 — the 80% path** | `Otlp{Grpc,Http}{Receiver,Exporter}`, `Pipeline`, `Transforms`, `BatchingProcessor`, the model `of(...)` factories and core records (`TracesData`, `MetricsData`, `LogsData`, `Resource`, `Attributes`, `Span`, `Metric`, `LogRecord`, `ConsumeResult`).                                            |
| **Tier 2 — advanced**     | `FanOut`, `Connectors`/`FailurePolicy`, `TelemetryTap`/`TapOptions`, `OverflowPolicy`, `PipelineHandle`, `Source`, `Telemetry`, `Drainable`/`ForceFlushable`, `Exporter`/`Receiver`, `Transform`, `ThrowingConsumer`, config (`ClientConfig`, `ServerConfig`, `Tls`, `Compression`, `RetryPolicy`). |
| **Provider SPI**          | `spi.OtlpClient`, `spi.OtlpServer`, `spi.Dispatchers`.                                                                                                                                                                                                                                              |
| **Experimental**          | `ProfilesData` and all profiles entry points (§4).                                                                                                                                                                                                                                                  |
| **Internal (excluded)**   | `Signal`, `BatchMergers`, the transport bases (`spi.internal`), `codec`, proto.                                                                                                                                                                                                                     |

- [ ] Manifest generated and checked in; contains **zero** internal/experimental-leak types in the covered tiers.
- [ ] CI diff gate active.
- [ ] `docs/public-api.md`'s type table reconciled to contain exactly the Application-tier types.

---

## 6. Out of 1.0 scope (deferred — additive, feedback-driven)

Non-breaking to add after 1.0, so deliberately not in the freeze. Listed to record they were considered, not as a committed roadmap — each is revisited only if real usage asks for it.

- **Ref-counted exporter auto-drain** (§3) — facets carry their exporter's lifecycle via ref-counted teardown, removing the explicit-`owns` step for the common case. The 1.0 loud-leak mitigation covers the gap until then.
- `endpoint(String url)` / `endpoint(URI)` builder overloads.
- `scheduleDelay(Duration)` (the batcher flush cadence; the current default stays).
- A synchronous `consume` helper that unwraps failures (fix the *doc pattern* now — stop modeling raw `.toCompletableFuture().join()` — and defer the API).
- `tap().<signal>().forEach(...)` / `subscribe(Consumer)` convenience.
- `Resource.merge(...)`.
- A signal-aware `to(exporter)` overload + a purpose-built public `SignalKind` discriminator (only if feedback wants it; the §3 ownership work removes the need that would motivate it).
- A re-introduced custom batcher `scheduler(...)` knob, if asked for.
- An optional `otlp4j-otel-sdk` adapter module bridging to OpenTelemetry SDK types.

---

## 7. Sequencing

Two waves. Wave 1 is §2/§4/§5 (breaking or surface-defining; lands before the freeze) plus the §3 mitigation. Wave 2 is §6 (post-1.0). The principle within Wave 1: **touch each file's logic once, and let the compiler-verified blind sweeps land last** — the widest rename last (javac flags every miss), the behavioral work first (for soak).

| #   | Step                                                                                                                                                                                                           | Risk                        | Note                                                                                                                                                                                           |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | **Behavioral/contract work first (for soak)** — the §2.4 default flips (reject-retryable, loopback bind) and the §3 loud-leak mitigation                                                                       | Moderate (behavioral)       | Land behavior changes early so they soak. The §3 auto-drain redesign is deferred (§6), so no risky concurrency work is in the cut.                                                             |
| 2   | **Processor/receiver consolidation, one pass** — `OverflowPolicy`, internalize `Signal`/`BatchMergers`, `flushThreshold`/`queueCapacity` params, **and** the batcher-builder trims (`dropCounter`/`scheduler`) | Mechanical                  | All rewrite `BatchingProcessor`/its builder together — one edit so the file is touched once; `module-info` loses its `Signal`-era exports here. `OverflowPolicy` also sweeps the tap path.     |
| 3   | **Relocate transport bases (§2.1) + qualified export**                                                                                                                                                         | Tricky (JPMS)               | File move + `module-info` + imports. Like `codec`, api's `module-info` will need `@SuppressWarnings("module")` for the reactor-order lint. Failures surface at transport compile, so not last. |
| 4   | **Core renames `PipelineHandle`/`ForceFlushable` + remove `peek` + cut `ConsumeResult.rejected(String)`**                                                                                                      | Mechanical, collision-prone | `PipelineHandle` is IDE-rename-only. (`Exporter<T>` is keep+document — a docs task that interleaves freely.)                                                                                   |
| 5   | **`TracesData` rename**                                                                                                                                                                                        | Mechanical, widest          | Compiler-verified across ~47 files; the safest possible last move before the freeze.                                                                                                           |

Hygiene that isn't a rename (§4 docs, the policy doc, `@Experimental` coverage, the packaging gate) interleaves freely.

**Blast radius (verified counts):**

| Change                                  | Occurrences / files                                   | Modules                                            |
| --------------------------------------- | ----------------------------------------------------- | -------------------------------------------------- |
| `TracesData` rename                     | ~301 occ / ~47 files                                  | model, api, codec, grpc, http, testing, samples    |
| `PipelineHandle` rename                 | ~17 api files                                         | api (⚠ `Flow.Subscription` alongside — IDE rename) |
| `ForceFlushable` rename                 | ~9 api files                                          | api                                                |
| `OverflowPolicy` unify                  | ~28 occ / ~12 api files                               | api (not samples)                                  |
| `flushThreshold`/`queueCapacity` params | ~7 main + ~26 test sites                              | api                                                |
| Internalize `Signal`/`BatchMergers`     | api only; no external consumer                        | api                                                |
| Relocate transport bases                | api + grpc + http + `module-info`                     | api, grpc, http                                    |
| Remove `peek`                           | decl + impl + 1 test + 3 prose spots                  | api + docs                                         |
| Cut `ConsumeResult.rejected(String)`    | 1 main + ~6 test files                                | model, api, transports                             |
| Hide batcher knobs                      | `BatchingProcessor.Builder` + tests                   | api                                                |
| §3 loud-leak mitigation                 | exporter + sample + `public-api.md`/`architecture.md` | api, samples, docs                                 |
| §2.4 default flips                      | `SignalSource`/`ServerConfig` + tests + docs          | api, docs                                          |

**Verification.** `./mvnw verify` runs surefire, JaCoCo, the `lint-javadoc` profile, and the JDK-25 enforcer. Two gaps the compiler does **not** cover:

- `lint-javadoc` validates `///` markdown links (`[TracesData]`, …) — grep `///` references after each rename.
- **Plain markdown is unvalidated by any build step.** A rename/removal/internalization that misses `docs/public-api.md`, `docs/architecture.md`, or `README.md` passes the entire build green (e.g. internalizing `BatchMergers` would otherwise leave it named in `architecture.md`). Add an explicit grep sweep of `docs/*.md` + `README.md` for every renamed/removed/internalized type to each step.

Run `./mvnw -q -pl <module> -am test` per step; full `verify` (incl. the codec round-trip tests in the `TracesData` blast radius) at each landing.

---

## 8. Definition of done (freeze gate)

The freeze is auditable when all of the following are true:

- [ ] **Compatibility policy** (§1) written, adopted, published as `docs/compatibility.md`, linked from the README — including the tuning-vs-contract default split.
- [ ] **All §2 breaking items** landed or consciously parked (the §9 decisions are settled).
- [ ] **§3 ownership** loud-leak mitigation landed; the ref-counted auto-drain redesign deferred to 1.x (§6).
- [ ] **Frozen-surface manifest** (§5) checked in, clean against tier intent, CI diff gate active.
- [ ] **`@Experimental` coverage** complete (manifest-driven); `@Internal` marker added; `spi/package-info.java` states the SPI evolution rule.
- [ ] **Packaging gate** green: `samples` deploy-skipped; `testing` modularized; artifact ↔ module-name check passing; reproducible timestamp and release `revision` set.
- [ ] **Docs reconciled**: `public-api.md` type table matches the Application tier; the `docs/*.md` + `README.md` grep sweep is clean for every renamed/removed/internalized type; every example uses the safe build → wire → `start()` order and the documented per-destination-queue fan-out; no change-narration or "formerly" wording in user-facing docs.

---

## 9. Decisions log (resolved)

The calls that were open are settled as follows; the body sections above are marked Decided to match.

| #   | Decision                             | Resolution                                                                                    |
| --- | ------------------------------------ | --------------------------------------------------------------------------------------------- |
| 1   | Unattached-source default (§2.4)     | **Reject-retryably**; accept-and-drop only as an explicit per-signal `discard(...)` opt-in.   |
| 2   | Receiver bind default (§2.4)         | **Default to loopback**; wildcard becomes explicit.                                           |
| 3   | Transport-base placement (§2.1)      | **Internal SPI** (`spi.internal`), qualified-exported to the two transports only.             |
| 4   | `Exporter<T>` (§2.1)                 | **Keep** as the supported custom-exporter SPI and document it; govern by default-only growth. |
| 5   | Overflow enums (§2.2)                | **Merge** into `OverflowPolicy {DROP_OLDEST, DROP_NEWEST, BLOCK, FAIL}` with per-site docs.   |
| 6   | Exporter-ownership fix (§3)          | **Loud leak-detection for 1.0**; ref-counted (not memoized) auto-drain deferred to 1.x (§6).  |
| 7   | `spi.Dispatchers` extensibility (§4) | **Keep the record**; SPI policy states a new signal is a major-version event.                 |
