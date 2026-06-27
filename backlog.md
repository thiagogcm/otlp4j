# Backlog

## Metadata

Export shape: each epic and issue has a stable identifier, title, labels, acceptance criteria, and implementation context. Epic dependencies are declared in the epic and dependency matrix sections. Epic identifiers are semantic uppercase slugs. Issue identifiers append a two digit local sequence to the epic identifier.

Evidence cites stable code symbols or tests rather than line numbers.

## Contract Ownership

- `docs/public-api.md` owns the public API contract, lifecycle contract, type-system contract, and usage examples.
- `README.md` owns top-level project positioning and first-contact examples.
- `docs/architecture.md` owns implementation architecture and lifecycle rationale.
- This backlog owns sequencing and verification only.

## Dependency Matrix

| Epic     | Depends on | Related            | Blocks |
| -------- | ---------- | ------------------ | ------ |
| BOUNDARY | none       | PUBLIC-API-HYGIENE | DOCS   |
| DOCS     | none       | BOUNDARY           | none   |

## Epic: BOUNDARY

Title: Public boundary and module stability

Depends on: none

Related: PUBLIC-API-HYGIENE

Labels: code, docs

Intent: Remove contradictions between the JPMS export surface and the documented public API. Users should be able to tell which packages are supported without reading implementation comments.

### Issue: BOUNDARY-01

Title: Resolve codec public/internal contradiction

Labels: code, docs

Acceptance criteria:

- `dev.nthings.otlp4j.codec` is either hidden behind qualified exports or documented as supported public API.
- `README.md`, `docs/public-api.md`, and `otlp4j-codec/pom.xml` use the same ownership language as `otlp4j-codec/src/main/java/module-info.java`.
- If codec remains internal, public signatures in the codec package are not advertised as stable and do not leak into the application API guide.
- If codec becomes supported, public facades avoid exposing generated proto details unless that exposure is deliberate and documented.
- The reactor build still succeeds after the export decision.

Context:

- Keeping codec internal means restoring qualified exports to the transport modules or introducing another non-public module boundary that works with the build.
- Making codec supported means adding a deliberate facade and removing internal-only language from docs and metadata.

Evidence / gap:

- `module dev.nthings.otlp4j.codec` exports `dev.nthings.otlp4j.codec` unqualified.
- `docs/public-api.md`, `README.md`, and `otlp4j-codec/pom.xml` describe generated proto and codec packages as implementation details.
- `TraceMapper`, `MetricsMapper`, `LogsMapper`, `ProfilesMapper`, and `SignalResponses` are public codec types and expose generated proto types in signatures.

## Epic: DOCS

Title: Documentation accuracy and first-run UX

Depends on: none

Related: BOUNDARY, EXAMPLE-COVERAGE

Labels: docs

Intent: Make the public guide task-oriented and keep it synchronized with the hardened API surface so users can start, transform, export, and shut down pipelines correctly.

### Issue: DOCS-01

Title: Add a task-oriented Start Here guide

Labels: docs

Acceptance criteria:

- `docs/public-api.md` starts with a compact task index or Start Here section.
- The guide includes receive-and-print, receive-transform-export, and construct-and-export paths.
- Transform examples use copy-modify APIs when those APIs are available.

Context:

- Keep the existing reference content, but add an entry point for users who need a first working path.
- Sequence transform examples after model copy helpers to avoid teaching verbose reconstruction.

Evidence / gap:

- `docs/public-api.md` currently begins with package and concept reference material.
- `README.md` has a compact first-contact example, but the public API guide does not have a dedicated Start Here section.
- `docs/public-api.md` contains receive, transform, export, and model snippets, but they are not grouped as first-run task paths.

### Issue: DOCS-02

Title: Expand lifecycle cheat sheet and ownership examples

Labels: docs

Acceptance criteria:

- Public docs include a lifecycle cheat sheet for receivers, subscriptions, processors, exporter facets, and explicit owners.
- Examples show why `Stage.owns(...)` is needed for count sinks or other hidden downstream resources.
- Examples cover batcher auto-ownership, fan-out, and exporter-facet ownership.
- Cheat-sheet guidance explains which resources are auto-collected as `AutoCloseable` terminals or fan-out peers and which resources remain hidden behind lambdas/connectors.

Context:

- Lifecycle ownership is a frequent source of subtle leaks or double-shutdown confusion.
- Keep examples runnable or directly adaptable to sample code.

Evidence / gap:

- `Pipeline.Stage.owns`, `Pipeline.Stage.to`, exporter facets, `BatchingProcessor`, and `Connectors` encode distinct ownership behavior.
- `docs/public-api.md` documents shutdown order, exporter facets, fan-out ownership, batchers, and count-sink hidden ownership, but examples should be grouped into a concise cheat sheet.
- `README.md` now includes exporter-facet lifecycle guidance, but the public API guide remains the contract owner for the complete lifecycle reference.

### Issue: DOCS-03

Title: Document thread-safety and nullness summaries

Labels: docs

Acceptance criteria:

- Public docs summarize thread-safety for receivers, exporters, batchers, subscriptions, and taps.
- Public docs summarize the nullness contract after systematic `@NullMarked` coverage lands.
- The summary names any intentionally nullable builder or configuration fields.

Context:

- This should follow nullness cleanup so docs describe the implemented contract.
- The summary should be concise and linked from lifecycle or public API reference sections.

Evidence / gap:

- `OtlpGrpcReceiver`, `OtlpHttpReceiver`, `OtlpGrpcExporter`, `OtlpHttpExporter`, `BatchingProcessor`, `Pipeline.PipelineSubscription`, and `TelemetryTap` define the concurrency-sensitive surface.
- Public docs include lifecycle guidance but do not provide a single thread-safety and nullness summary.
- Systematic `@NullMarked` coverage and targeted `@Nullable` annotations are now in place, so the nullness summary can be authored against the implemented contract.

## Cross-Cutting Pillars

### Pillar: PUBLIC-API-HYGIENE

Blocks: BOUNDARY, DOCS

Summary: Public API additions should be deliberate, consistently documented, nullness-annotated, and compatible with the module boundary. This pillar keeps pre-1.0 hardening focused on stable ergonomics rather than expanding surface area opportunistically.

### Pillar: EXAMPLE-COVERAGE

Blocks: DOCS

Summary: New ergonomics should be represented by examples that exercise realistic receive, transform, export, and shutdown flows. Examples are the verification bridge between API shape and user comprehension.
