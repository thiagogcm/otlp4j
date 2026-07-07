# Phase 5 Follow-Up: Remaining Work

Phase 5 of the overhaul in `otlp4j-report.md` was deliberately stopped after Lot 6 of the twelve
lots in `docs/phase5-design.md`. This document records exactly what landed, what remains, and how to
resume. The design document is the implementation-ready source of truth for every remaining lot; this
file is the map, not the territory.

## What landed (Lots 1 through 6)

Each lot left the full reactor green under `./mvnw -B verify`, and the tree was verified green again
before the commit that carries them.

- Lot 1, core Phase 5 surface: `SignalSupport` relocated from `otlp4j-service` to the exported
  `dev.nthings.otlp4j.core.component`, `SignalDescriptor.split(T, long)` for max-batch-size
  splitting, the `RoutingMetadata` value type and the `ROUTING_METADATA` `PayloadView` constant, the
  `dev.nthings.otlp4j.core.config` package (`ConfigNode`, `ConfigBinder`) for declarative binding,
  the `Extensions` resolver threaded through an additive `CreateContext` argument, and the
  `dev.nthings.otlp4j.core.auth` SPI.
- Lot 2, codec routing metadata: both `ProtoRequestPayload` and `DomainRequestPayload` supply
  `ROUTING_METADATA` (the proto path via resource-level attribute mapping), with parity tests and a
  no-materialisation proof test (`RoutingMetadataNoMaterialisationTest`).
- Lot 3, service extension resolution: the graph builder constructs extensions first and hands the
  `Extensions` resolver to every factory through `CreateContext`; extensions outlive the data path in
  lifecycle order (`ExtensionResolutionTest`).
- Lot 4, the `otlp4j-components` module: `BatchProcessor` (count, bytes, timeout, and metadata-key
  triggers per `BatchPolicy`, using `SignalDescriptor.merge` and `split`, with the profiles
  distinct-dictionary merge rejection handled) and the internal `DeliveryQueue` helper (bounded
  delivery, `waitForResult`, drain-outcome-keyed degraded window, `OverflowPolicy`). No
  `SendingQueue`, no `AckMode`, per the report.
- Lot 5, the memory limiter processor: soft and hard thresholds with an injectable gauge; refusals
  are retryable with retry-after.
- Lot 6, routing and filtering processors: built on a shared match engine that reads only
  `PayloadView.ROUTING_METADATA` and `ConsumeContext.metadata()`, proven never to touch the domain
  view unless the explicit domain-predicate escape hatch is configured.

## What remains (Lots 7 through 12, then gate and review)

The lot specifications, file inventories, pom and `module-info` edits, and test lists are in
`docs/phase5-design.md`. Summary of each remaining lot:

- Lot 7, deriving connector: the `DerivingConnector` helper and a built-in
  `SpanCountConnectorFactory` in `otlp4j-components`; rebase `otlp4j-samples` onto it (components
  dependency plus `requires dev.nthings.otlp4j.components;`, `OtlpE2eDemo` update, retire the samples
  seed `CountConnectorFactory`).
- Lot 8, server-side auth wiring: widen `Dispatchers` to carry `ClientMetadata`, add the Armeria HTTP
  decorator and gRPC interceptor that invoke the configured authenticator, propagate the
  authenticated principal into `ConsumeContext`, and map unauthenticated requests to the correct wire
  status per protocol.
- Lot 9, built-in authenticators and client credentials: concrete authenticator extensions and
  client-side credentials feeding `ClientHeaders.resolve` on exporters.
- Lot 10, the `otlp4j-config-yaml` module: snakeyaml 2.3 (an explicit JPMS module), the
  Collector-style YAML loader compiling to `GatewayConfig` through `ComponentRegistry` and the
  per-factory `ConfigBinder`s from Lot 1, `${env:...}` substitution, unknown-key and
  unknown-component diagnostics, transport binders, and coverage wiring.
- Lot 11, the component conformance test kit: core-only abstract bases in `otlp4j-testing` and the
  graph/codec harness in `otlp4j-components` test sources (the placement respects the reactor
  dependency constraint that codec already test-depends on testing).
- Lot 12, samples and exit-criteria proof: a `gateway.yaml` sample, a YAML end-to-end boot test
  covering a receiver to processors to exporter pipeline and a connector-mediated pipeline pair, and
  a proof test registering a third-party component without touching any core module.

After Lot 12, the phase closes with the gate defined by the report's Phase 5 exit criteria:

1. A Collector-like YAML service config expresses plain and connector-mediated pipelines and boots
   end to end.
2. Third-party components are addable without touching core modules.
3. The Armeria conformance and policy suites stay green and the report's non-goals stay out of scope
   (no persistent queue, no config hot-reload, no OTLP/HTTP JSON encoding, no multi-tenant
   isolation).

The established review shape for closing the phase: a gate agent verifying the exit criteria, three
reviewers (batching and backpressure concurrency, auth and config semantics, API surface and build
integrity), adversarial verification of every finding, and a fixer for confirmed findings.

## Notes for the resumer

- The delivered Lot 1 `ConfigBinder`/`ConfigNode` surface and the Lot 3 `Extensions` resolver were
  built for Lots 8 through 10; they currently have no production consumer beyond the components
  module, which is expected and harmless.
- The `core.auth` SPI is contract-only until Lot 8 wires it into the Armeria transport.
- `BatchPolicy.metadataKeys` partitioning works against `ConsumeContext.metadata()`; the receiver
  populates it fully only once Lot 8 lands the `Dispatchers` widening, so metadata-key batching is
  exercised today only through direct consumer tests.
