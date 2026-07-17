# Architectural Assessment and Cleanup Refactoring

Date: 2026-07-16. Scope: all main-source modules at commit `98319e3`. Method: full read of every main-source file in core, model, codec, service, components, transport-common, transport-otlp, testing, samples, and benchmarks, with repo-wide usage greps behind every deletion claim. This document first assesses the architecture as it actually is (modules, layers, abstractions, boundaries, responsibilities), then proposes a cleanup refactoring. Breaking changes are assumed acceptable throughout; nothing here preserves compatibility with the current 0.1.0-SNAPSHOT surface.

## Part 1: Assessment

### 1.1 The architecture as built

The overhaul driven by `otlp4j-report.md` (Phases 1 through 5 Lot 6) produced a faithful Java translation of the OpenTelemetry Collector's component model. The layering, from the bottom up:

| Layer | Module | Responsibility |
| --- | --- | --- |
| Domain | `otlp4j-model` | Immutable records for the four OTLP signals, builders, attribute model. Proto-free. |
| Contracts | `otlp4j-core` | Component SPI (five kinds plus factories), signal identity and batch algebra (`SignalDescriptor`), the opaque `TelemetryPayload` with its view system, consumer SPI, delivery results, neutral policies, self-observability SPI. Proto-free. |
| Wire boundary | `otlp4j-proto`, `otlp4j-codec` | Generated protobuf (qualified exports only) and the proto/domain mappers behind two `TelemetryPayload` implementations (`ProtoRequestPayload`, `DomainRequestPayload`). |
| Orchestration | `otlp4j-service` | `GatewayConfig` to running `GatewayRuntime`: JGraphT DAG, 18 validation diagnostics, topological start/shutdown, fan-out insertion, consumer guarding. |
| Transport | `otlp4j-transport-common`, `otlp4j-transport-otlp` | The `OtlpClient`/`OtlpServer` SPI, adapter classes bridging wire objects into graph components, config records, and the single Armeria implementation carrying OTLP/gRPC and OTLP/HTTP. |
| Built-ins | `otlp4j-components` | Batch, memory-limiter, filter, and routing processors. |
| Verification | `otlp4j-testing`, `otlp4j-conformance`, `otlp4j-coverage`, `otlp4j-benchmarks` | Reusable conformance and policy suites, their Armeria instantiations, coverage aggregation, JMH. |

Dependency direction is strictly one-way and JPMS-enforced; no proto or gRPC type escapes the codec/transport boundary (verified by `ExporterRuntimeNegativeSpaceTest`). The payload design achieves its goal: service, components, and transport-otlp import zero `dev.nthings.otlp4j.model` types, so a same-protocol relay never materialises the domain.

### 1.2 What is genuinely good and should not be touched

- The payload/view system. `TelemetryPayload` + `PayloadView` + the two codec payloads with mutual lazy caching are parity-tested (`PayloadItemCountParityTest`, `RoutingMetadataParityTest`, `RoutingMetadataNoMaterialisationTest`) and every view has production consumers. This is the load-bearing idea of the whole redesign.
- The service graph runtime. Topological start/stop derived from one DAG, rollback on start failure, and a validation layer where all 18 diagnostic codes are actually emitted. The five graph `Node` types genuinely differ (different factory signatures, different sharing semantics) and should stay.
- The delivery-result model, the context objects (`CreateContext`, `ConsumeContext`, `ShutdownContext`, `FlushContext` all have real multi-module consumers), `SharedComponents`, `NetworkBound`, and the `GatewayMetrics` instrument-handle SPI.
- The Armeria internals: `ArmeriaRetryRules`, `GrpcPushback`, `RetryInfoCodec`, `HttpProtocolMapping`, `GrpcServiceAdapters`, `AdmissionControl`. Single-purpose, real machinery.
- The verification estate: conformance and policy suites, the negative-space test, the two JMH harnesses.

### 1.3 Where the complexity is unjustified

The over-engineering clusters into four distinct failure modes, each with a different origin.

**A. Speculative verticals built for Phase 5 lots that never landed.** Lot 1 and Lot 3 deliberately pre-built surface for Lots 7 through 12, then the phase stopped at Lot 6. The result is complete subsystems with zero production consumers:

- The entire `core.auth` package (`ServerAuthenticator`, `ClientCredentials`, `AuthResult`, `AuthSources`). Zero references outside its own unit test. Exported from the module for nobody (Lot 8/9 scaffolding).
- The Extension vertical: `Extension`, `ExtensionFactory`, `Extensions`, `CreateContext.extensions`, `service/internal/BuiltExtensions`, `service/config/ExtensionEntry`, the assembler/registry/builder paths, and `ExtensionResolutionTest`. The only `Extension` implementations in the repo are test fakes; the only intended real ones were the unbuilt authenticators.
- The declarative-config SPI: `core.config.ConfigNode` and `ConfigBinder`, plus the four `bind()` implementations in `otlp4j-components`. No loader exists (Lot 10 scaffolding); `bind()` is called only by the components' own factory tests.
- `otlp4j-components` itself is orphaned: `BatchProcessorFactory`, `MemoryLimiterProcessorFactory`, `FilterProcessorFactory`, and `RoutingProcessorFactory` have zero usages outside the module. Samples and benchmarks hand-roll their own processors instead. The module pom even advertises "the deriving connector helper, and the built-in authenticator extensions", neither of which exists.
- `Connector` has exactly one implementation, in samples (`CountConnectorFactory`); the `DerivingConnector` helper it was waiting for is unbuilt Lot 7.

**B. Debris from the Phase 4 transport consolidation.** The multi-transport abstraction shape survived the removal of the transports that justified it:

- `spi.OtlpClient`, `spi.OtlpServer`, `spi.Dispatchers`: one production implementation each (`ArmeriaOtlpClient`, `ArmeriaOtlpServer`, `ServerReceiver`). The test doubles do not implement the SPI (the `ProgrammedOtlpServer` family is a standalone fake on raw grpc-netty/HttpServer), so the seam is not even exercised by tests. The SPI's own Javadoc concedes callers bypass it.
- `AbstractExporterBuilder`/`AbstractReceiverBuilder`: self-typed CRTP bases with exactly one concrete subclass each; the generic self-type and the abstract `newClient`/`newServer` seam exist for subclasses that were deleted.
- Two parallel config families for the same knobs: `OtlpExporterConfig`/`OtlpReceiverConfig` (graph-facing) hand-copy 8 and 7 fields into `ClientConfig`/`ServerConfig` (wire-facing), duplicating validation, and the exporter path materialises the same data three times (`OtlpExporterConfig` to `ClientConfig` to `ClientConfig.Builder` to `ClientConfig`) before Armeria sees it. The timeout knob is modelled three ways along the way (`TimeoutPolicy`, two `Duration` fields, Armeria's response/attempt timeouts).
- Three fully-plumbed knobs the sole implementation silently ignores: `connectTimeout` (client), `handshakeTimeout` and `serverExecutor` (server). Each carries a builder setter, record validation, a second record field, and Javadoc describing behavior that never happens. These are documented lies.
- A dead public interface: `receiver.TelemetryTap` has zero implementors and zero callers; the working tap (`ReceiverTap`) duplicates its surface without implementing it. The whole unexported `dev.nthings.otlp4j.receiver` package exists to hold it plus `TapOptions`.
- Four near-identical ~50-line factories (`OtlpArmeriaGrpcExporterFactory` and the three siblings) differing only in default port, type string, and which builder method they call.
- Package archaeology: transport-common owns `dev.nthings.otlp4j.config`, `.spi`, and `.receiver`, names kept "so callers compile unchanged" from the deleted `otlp4j-api` era. `ClientExporter` carries 7 constructors, 4 of them existing to keep old test paths green.
- The exporter consume path is three wrappers deep before Armeria: `ExporterComponentAdapter` delegates to `ClientExporter.Facet` which calls `ArmeriaOtlpClient`, with `ExporterComponentAdapter` and `Facet` implementing the same consume contract in series.

**C. Ceremony in the component SPI and its service-side reflection.** The Collector's factory metadata was imported wholesale, including the parts this library never acts on:

- `StabilityLevel`: every factory must declare it; no production code ever reads it. Same for `SignalDescriptor.development()` (redundant with `stability()`), and `DeliveryReason.OK`/`PARTIAL_SUCCESS` (structurally unreachable: `reason` exists only on `Rejected`).
- The five-kind structure is spelled out three times in the service module: `GatewayAssembler` (five maps), `GatewayConfig` (same five maps), `ComponentRegistry` (169 lines of trivial forwarding over those maps, instantiated once, with an unused `config()` getter). The five `*Entry` records each carry dead `kind()`/`type()` methods (ten dead methods); only `create()` and `ConnectorEntry.supportedPairs()` are used.
- Every processor factory in the repo copies the same ~10-line unchecked-cast helper pair (`payloadConsumer` + `castProcessor`), six times across components, samples, and benchmarks.
- "A factory that needs no config still must declare a config type": three private `EmptyConfig implements ComponentConfig` copies exist in samples, benchmarks, and conformance.
- A trivial in-memory terminal exporter costs ~85 lines of factory/exporter/provenance boilerplate (see `OtlpE2eDemo.CapturingExporterFactory`), and reading a receiver's bound port is a four-step `instanceof NetworkBound` dance repeated identically in three modules.
- Wiring is stringly-typed and double-entered: every component is registered under a `ComponentId` and then those ids are repeated inside `PipelineConfig` lists. A typo is a runtime validation error, not a compile error.

**D. Local dead weight and misplacements.** Individually small, collectively noisy:

- Dead methods in service (13 total: the entry `kind()`/`type()` ten, `ServiceGraph.componentsOf`, `ServiceGraph.source`, `ComponentRegistry.config()`, plus `FanOutConsumer.anyPeerMutates()`).
- `TelemetryPayload.memoryMode()` and `PayloadMemoryMode`: the documented beneficiary (`FanOutConsumer`) never calls it; only tests read it. `DomainRequestPayload.batch()` is a "transitional" accessor superseded by `view(DOMAIN_IMMUTABLE)`, now test-only. The model's `Experimental` annotation has zero readers anywhere.
- The four codec mappers and `DeliveryResultResponses` are `public` with no caller outside the package (everything flows through `SignalCodec`); visibility is inconsistent even within each mapper.
- `codec.Transports` (socket bind-address and mTLS-material validation) is a transport concern living in the marshalling module, used only by transport-otlp.
- `GraphNodeBoundary` is public in the exported `service` package purely so `service.internal` can reach it; it is an internal normalization helper, not a node concept.
- `core` requires `org.slf4j` only because `Component.close()`'s default body logs and because `LoggingGatewayMetrics` (a self-described sample adapter) lives in the API module. The `telemetry` package also sits at `dev.nthings.otlp4j.telemetry` while every sibling is under `core.*`.
- `ProtoEnum` unifies almost nothing: each implementing enum still re-declares its own cached `VALUES` array and static `fromNumber` wrapper.
- `internal.Protocol` in transport-otlp is public inside an unexported package, and byte-identical to `testing.conformance.Protocol`.
- `components.internal.Sizer` is an enum-with-abstract-method selecting between two longs at two call sites; `DeliveryQueue` is framed as "the reusable bounded delivery boundary" but has one caller.
- Documentation actively describes a dead architecture: `README.md` entry points (`OtlpGrpcReceiver`, `Pipeline`, `Transforms`, `Batcher`) and all of `docs/architecture.md` predate Phase 1. `docs/public-api.md` likewise. The samples `module-info` comment describes a downcast the demo no longer performs.

### 1.4 The structural diagnosis

Three sentences of diagnosis before the plan. First, the library carries roughly a module and a half of scaffolding for features that do not exist (auth, extensions, declarative config, a second transport), which is the textbook cost of building platform seams ahead of their first consumer. Second, the transport stack still has the shape of a framework hosting N transports when it is in fact one transport, so every real object is wrapped in one neutral adapter and one abstract base too many. Third, the parts that process telemetry (payloads, graph, Armeria internals) are healthy; the parts that describe and assemble components (factories, configs, entries, registries) are where duplication and ceremony concentrate.

## Part 2: Cleanup refactoring

Six work packages, ordered so each leaves the reactor green under `./mvnw -B verify`. Estimated net deletion is 3,500 to 4,500 main-source lines out of ~20,000 plus a proportional test surface, with zero capability loss except where a decision point explicitly says otherwise.

### Decision points (choose before starting)

1. **Cut or finish the Phase 5 remainder.** This plan recommends cutting the speculative scaffolding now (WP2). `docs/phase5-design.md` remains the blueprint; when auth or YAML config is actually built, its contracts return together with their first consumer, redesigned against what the consumer really needs. Pre-built seams that sat unused for months are exactly what this assessment found rotting; the pattern should not be re-run. If instead you intend to build Lots 8 through 10 imminently, skip WP2 and jump those lots to the front of the queue, because the scaffolding is only defensible with its consumers attached.
2. **Connector stays.** Unlike auth and extensions it has a live end-to-end consumer (the demo's span-count connector plus a metrics pipeline) and an irreplaceable architectural role: it is the only way pipelines compose. WP5 promotes the sample connector into `otlp4j-components` (the small useful kernel of Lot 7) rather than deleting the SPI.
3. **Naming: drop "Armeria" from public type names.** There is one transport; the backend is an implementation detail the names currently leak. `OtlpArmeriaGrpcReceiverFactory` becomes `OtlpGrpcReceiverFactory`, and so on. If you value advertising the backend, keep the names and skip that rename; nothing else depends on it.

### WP1: Dead-code and honesty sweep (no design changes, fully mechanical)

Delete, with their tests where test-only:

- Service: the ten `*Entry.kind()`/`type()` methods, `ServiceGraph.componentsOf`, `ServiceGraph.source`, `ComponentRegistry.config()`, `FanOutConsumer.anyPeerMutates()`, `Node.component()` (replace the one caller with `!(node instanceof FanOutNode)`).
- Core/codec: `TelemetryPayload.memoryMode()` and `PayloadMemoryMode`, `DomainRequestPayload.batch()` (migrate its two tests to `view(DOMAIN_IMMUTABLE)`), `SignalDescriptor.development()`, `DeliveryReason.OK` and `PARTIAL_SUCCESS`, the model `Experimental` annotation (fold into Javadoc on `ProfilesData`), `ProtoEnum` (each enum keeps its own `fromNumber`; the interface buys nothing).
- Transport: `TelemetryTap` and the `dev.nthings.otlp4j.receiver` package (move `TapOptions` next to its only consumers in `transport.common`), the three dead knobs `connectTimeout`, `handshakeTimeout`, `serverExecutor` end to end (setter, both record fields, validation, Javadoc). If any is wanted later, wire it into Armeria in the same commit that restores it; `connectTimeout` is a one-liner on `ClientFactory` when someone actually needs it.
- Visibility: make `TraceMapper`, `MetricsMapper`, `LogsMapper`, `ProfilesMapper`, and `DeliveryResultResponses` package-private; unify the split method visibility inside each mapper. Move `GraphNodeBoundary` to `service.internal`. Make `transport.otlp.internal.Protocol` package-private.
- Text: fix the components pom description, the stale samples `module-info` comment, and delete or quarantine the false parts of `README.md` pending WP6.

### WP2: Cut the speculative verticals (decision point 1)

- Delete `core.auth` (four types, package-info, export, `AuthTypesTest`).
- Delete the Extension vertical: `Extension`, `ExtensionFactory`, `Extensions`, the `extensions` field of `CreateContext`, `BuiltExtensions`, `ExtensionEntry`, `GatewayAssembler.extension(...)`, the `extensions` maps in `GatewayConfig`/`ComponentRegistry`, `ServiceOptions.extensions`, the extension arm of the builder and runtime lifecycle, `ExtensionResolutionTest`, and `ComponentKind.EXTENSION`.
- Delete `core.config` (`ConfigNode`, `ConfigBinder`) and the four `bind()` implementations plus `MatchSpec`'s node-parsing constructors in `otlp4j-components`.
- `StabilityLevel`: delete the enum and the mandatory `ComponentFactory.stability()`. When a stability gate is actually wanted, it returns as behavior (for example the builder refusing `DEVELOPMENT` components without an opt-in), not as inert metadata.

What this buys: `otlp4j-core` drops from 67 files to roughly 50 and every remaining exported type has a production consumer. What it costs: Lots 8 through 10 start from the design doc instead of from pre-built contracts. That trade is the point.

### WP3: Flatten the transport stack into one module

Merge `otlp4j-transport-common` and `otlp4j-transport-otlp` into a single **`otlp4j-transport`** module (JPMS `dev.nthings.otlp4j.transport`), then collapse inside it:

- Delete the `spi` package (`OtlpClient`, `OtlpServer`, `Dispatchers`). `ArmeriaOtlpClient`/`ArmeriaOtlpServer` stop implementing an interface nobody else implements; `ServerReceiver` passes a lambda or itself where `Dispatchers` was the parameter type. The real extension seam for a hypothetical second transport is `ReceiverFactory`/`ExporterFactory` in core, which is the seam the graph actually consumes; an interface below that duplicated it.
- Delete the CRTP `AbstractExporterBuilder`/`AbstractReceiverBuilder`; fold their fluent surface directly into `OtlpExporter.Builder`/`OtlpReceiver.Builder` (the renamed `OtlpArmeria*` facades, decision point 3).
- One config family instead of two. Keep the graph-facing `OtlpExporterConfig`/`OtlpReceiverConfig` records as the single source of truth and have the client/server consume them directly; delete `ClientConfig`/`ServerConfig` and both `toClientConfig()`/`toServerConfig()` copy layers. `Tls`, `Compression`, `Headers`, `OtlpEnv` move from `dev.nthings.otlp4j.config` into the transport package (the "callers compile unchanged" rationale died with `otlp4j-api`). Express timeouts once, as core's `TimeoutPolicy`.
- Collapse the adapter chain by one hop: merge `ExporterComponentAdapter` into `ClientExporter.Facet` (a facet is already the per-signal graph-facing consumer; the extra adapter re-implements the same contract around it). Prune `ClientExporter` to the 2 or 3 constructors production needs and fix the tests that drove the other 4.
- Two factories instead of four: `OtlpReceiverFactory` and `OtlpExporterFactory`, each constructed with a `Protocol` (`grpc()`/`http()` statics for discoverability), deriving `type()` ("otlp"/"otlphttp") and default port from it. The per-protocol branching already lives once in `ArmeriaOtlpClient`/`ArmeriaOtlpServer`; the factories carry none of it.
- Absorb `codec.Transports` (bind-address, mTLS validation) into the transport module, restoring codec to pure marshalling.

### WP4: Slim the service module and fix the wiring ergonomics

- Delete `ComponentRegistry`; its trivial lookups fold into `GatewayBuilder` as private helpers over `GatewayConfig`'s maps. The exported `service.config` package then contains only what users can meaningfully touch.
- Keep `GatewayConfig` (the declarative artifact a future YAML loader compiles to) and keep the typed `*Entry.create()` seam (it is what makes instantiation cast-free), but the entries lose their dead metadata per WP1.
- **Handle-based wiring** (new concept, kills the stringly-typed double entry): registration methods on the builder return typed handles, and pipelines take handles:

  ```java
  var gw = Gateway.builder();
  var in      = gw.receiver("in", OtlpReceiverFactory.grpc(), receiverCfg);
  var enrich  = gw.processor("enrich", enrichFactory, cfg);
  var out     = gw.exporter("out", OtlpExporterFactory.grpc(), exporterCfg);
  gw.pipeline(PipelineId.of(TRACES), p -> p.from(in).via(enrich).to(out));
  GatewayRuntime runtime = gw.build();
  ```

  A handle is a thin record carrying the `ComponentId` plus the component kind in its type (`ReceiverHandle`, `ProcessorHandle`, `ExporterHandle`, `ConnectorHandle`), so referencing an unregistered or wrong-kind component becomes a compile error instead of one of the five runtime `UNKNOWN_*` diagnostics (which remain for the raw `GatewayConfig` path). `GatewayAssembler` disappears into this builder; `GatewayConfig` stays constructible directly for programmatic/declarative use.
- **No-config and inline components** (new concepts, kill the recurring boilerplate): a `ComponentConfig.NONE` singleton in core replaces the three private `EmptyConfig` copies, and static functional factories replace the 85-line factory ceremony for trivial components:

  ```java
  ExporterFactory.of("capture", (ctx, signal) -> payload -> { sink.accept(payload); return accepted(ctx); });
  ProcessorFactory.of("enrich", TRACES, batch -> enrich(batch));
  ```

  These live as statics on the existing factory interfaces in core; the samples and conformance seams rewrite onto them.
- `GatewayRuntime.boundPort(ComponentId)` (returns `OptionalInt`) replaces the four-step `NetworkBound` downcast dance copied across samples, benchmarks, and both conformance seams. `NetworkBound` itself stays.
- Extract the six copies of the processor-factory cast pair into one core helper (for example package-private statics on `ProcessorFactory`), so a factory author writes zero unchecked casts.
- Optional, lower value: split `GatewayBuilder`'s instantiation phase into a package-private collaborator if the class bothers you after the above lands; at ~500 remaining lines with the extension arm and registry gone, it is defensible as is.

### WP5: Make the built-in components real

The components module is the library's advertised value ("filtering and enrichment, batching, concurrent routing") and currently nothing uses it. Wiring it is cleanup in the truest sense, because orphaned code cannot be trusted:

- Rewrite the demo's hand-rolled enrich/redact/filter loops onto `FilterProcessor`/`RoutingProcessor` where they fit, and add `BatchProcessor` to the demo's forwarding pipeline. Where the built-ins cannot express the demo's transform (span-kind filtering), that is a product-gap signal to fix in the component, not a reason to keep parallel sample infrastructure.
- Promote the sample's `CountConnectorFactory` into `otlp4j-components` as `SpanCountConnectorFactory` (the useful kernel of Lot 7), leaving samples with no `support/` factories except the deliberately pedagogical `FunctionProcessorFactory`, which shrinks to a few lines once `ProcessorFactory.of` exists.
- Add one conformance-style test that boots a gateway with all four built-ins in a pipeline, closing the "exercised only by unit tests" gap.
- Local trims: collapse `Sizer` into two `if`s inside `BatchProcessor.shouldFlush`; keep `DeliveryQueue` but re-document it as the batch processor's delivery boundary (drop the "reusable" framing) or actually reuse it; `BatchProcessorConfig` keeps wrapping core policies (composition is fine) but stops re-validating what the policy records already validate.

### WP6: Documentation and naming truth

- Rewrite `docs/architecture.md` from scratch against the real design (the Part 1 table above is a skeleton); rewrite `README.md` entry points around `Gateway.builder()`, the two transport factories, and the built-ins; regenerate `docs/public-api.md`.
- Move `otlp4j-report.md` and the phase 5 docs under `docs/history/` so the repo root stops advertising a plan that WP2 partially supersedes; add a short `docs/phase5-followup.md` note recording what was cut and why.
- Renames for consistency: `dev.nthings.otlp4j.telemetry` becomes `dev.nthings.otlp4j.core.telemetry`; `LoggingGatewayMetrics` moves to samples (it is a sample adapter by its own Javadoc); `Component.close()`'s default stops logging (rethrow wrapped instead), which together with the move lets `otlp4j-core` drop `requires org.slf4j` entirely, making the contracts module dependency-free apart from jspecify and the model.
- Testing estate (optional, last): point both wire harnesses' shared `grpc-retry-pushback-ms` literal and per-signal proto request building at one small package-private helper inside `otlp4j-testing`; the two `Protocol` enums and the two programmed-response vocabularies stay (they sit on opposite sides of the layering and opposite ends of the wire; unifying them would couple more than it saves).

### Resulting module shape

| Module | Change |
| --- | --- |
| `otlp4j-model` | Unchanged surface; loses `Experimental`, `ProtoEnum`. |
| `otlp4j-proto` | Unchanged. |
| `otlp4j-core` | ~50 files: loses auth, config-binding, extensions, stability metadata, slf4j; gains `ComponentConfig.NONE`, factory statics, cast helper; telemetry package renamed under `core.*`. |
| `otlp4j-codec` | Mappers go package-private; loses `Transports`, `batch()`. |
| `otlp4j-service` | Loses registry, assembler, extension arm, 13 dead methods; gains handle-based builder and `boundPort`. |
| `otlp4j-transport` | New: merger of transport-common and transport-otlp; one config family, two factories, no SPI, no CRTP bases, no dead knobs; public names drop "Armeria". |
| `otlp4j-components` | Wired into samples/conformance; gains `SpanCountConnectorFactory`; loses `bind()`, `Sizer`. |
| `otlp4j-testing`, `otlp4j-conformance`, `otlp4j-coverage`, `otlp4j-benchmarks`, `otlp4j-samples` | Consumers of the above; samples shrink dramatically. |

Twelve reactor modules become eleven; the public API surface (exported packages times public types) shrinks by roughly a third while every surviving type has at least one production consumer.

### Sequencing and verification

Land the packages in order (WP1 through WP6); each is independently green-able. WP1 and WP2 are pure deletion and can ship the same day. WP3 is the largest single change and should be one focused branch. WP4's handle API is the only part with real design freedom; prototype it against the demo and the conformance seam before committing to the shape. After each package: `./mvnw -B verify`, plus the JMH smoke (`bench` profile, one fork, min iterations) after WP3 to confirm the adapter-flattening did not regress the passthrough path it exists to protect.

## Outcome

WP1 through WP6 landed on the `cleanup` branch, leaving the reactor green under `./mvnw -B verify` and the module count at eleven (plus the profile-gated `otlp4j-benchmarks`). The plan held, with a handful of deviations recorded here so the code and this document agree:

- **The inline processor seam carries a `PayloadFactory`.** `ProcessorFactory.of` is four arguments (`type`, `signal`, `transform`, `payloads`) rather than the three-argument sketch: the caller passes `Payloads.DOMAIN` as the emit seam, which is what keeps the factory in `otlp4j-core` without a codec dependency (core cannot name a codec type, so the emit function is injected).
- **`transport.internal.Protocol` stays public within its unexported package.** The `OtlpReceiverFactory`/`OtlpExporterFactory` facades and the internal Armeria client and server reference it across packages inside the module, so it could not go package-private; it remains encapsulated by the module boundary (the `internal` package is never exported).
- **`StabilityLevel` and `SignalDescriptor`'s stability field were both removed.** Cutting the mandatory `ComponentFactory.stability()` metadata (WP2) took the descriptor's redundant stability field with it; no production code read either.
- **The materializer is the package-private `GatewayCompiler`.** The builder and the declarative `Gateway.compile(config)` path both funnel through it; there is no separate public assembler type.
- **The DSL entry point is `Gateway.builder()` / `Gateway.compile(...)`.** `GatewayAssembler` disappeared into `GatewayBuilder`, whose registrations return the typed handles the pipelines wire; `GatewayConfig` remains directly constructible for the programmatic and future declarative paths.
- **`LoggingGatewayMetrics` moved to `otlp4j-samples`** (it was a sample adapter by its own Javadoc), and a dependency-free `RecordingGatewayMetrics` in `otlp4j-testing` took over the assertion role it had been serving in the transport and conformance tests. With that and the rethrow-on-`close` change, `otlp4j-core` dropped `requires org.slf4j` and its telemetry package moved under `dev.nthings.otlp4j.core.telemetry`.
