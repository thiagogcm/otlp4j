# Public API Feedback

Date: 2026-06-24

Scope: public Java API ergonomics, modern Java idioms, discoverability, composability, documentation, complexity, and differences from the official OpenTelemetry Go OTLP APIs.

## Executive Summary

`otlp4j` has a coherent public API direction: it is a proto-free, JPMS-modular OTLP gateway/pipeline library built around immutable domain records, signal-specific asynchronous sinks, explicit transports, and a small pipeline DSL. That positioning is strong and distinct from the official OpenTelemetry Java SDK. The best parts of the API are the module boundaries, the signal-specific `TraceSink`/`MetricSink`/`LogSink`/`ProfileSink` aliases, the `ConsumeResult` partial-success model, and the concrete `OtlpGrpcExporter`/`OtlpHttpExporter` and receiver builders.

The main risks are not conceptual. They are polish and long-term API stability risks: an internal `otlp4j-codec` package is exported despite being documented as an implementation detail, copy-modify operations on immutable telemetry records are harder than they should be, nullness annotations are inconsistent, the lifecycle ownership model is powerful but subtle, and defaults differ materially from official Go exporters. The documentation is unusually thorough for a young project, but a few stale or contradictory statements weaken discoverability.

The most important recommendation is to preserve the current high-level shape. Avoid turning `otlp4j` into a Java clone of the Go SDK exporters. Instead, tighten the public boundary, improve copy/transform ergonomics, clarify default and environment-variable differences from upstream, and add a small number of helper APIs for common gateway operations.

## Priority Recommendations

| Priority | Recommendation | Why |
| --- | --- | --- |
| P0 | Stop exporting `dev.nthings.otlp4j.codec` as public API, or explicitly reclassify it as supported. | Docs and POM say codec is internal, but `otlp4j-codec/src/main/java/module-info.java` exports it unqualified. This creates accidental public API before 1.0. |
| P0 | Fix documentation contradictions around codec exports, provider discovery, and existing HTTP receiver support. | Public-facing docs should reinforce the same mental model everywhere. |
| P1 | Add `toBuilder()` to primary builder-backed immutable records: `Span`, `Metric`, `LogRecord`, `NumberPoint`, `HistogramPoint`, `ExponentialHistogramPoint`, `Exemplar`, and possibly resource/scope wrapper records; add a builder or copy helper for `SummaryPoint`. | Most non-trivial transforms need copy-modify. Today callers rebuild large positional records or hand-nest wrappers. |
| P1 | Apply nullness consistently at package or module boundaries and annotate intentional nullable fields. | `org.jspecify` is exposed transitively, but only some classes are `@NullMarked`; nullable APIs such as `ServerConfig.serverExecutor` are implicit. |
| P1 | Decide and document whether Go-like secure/retry defaults are intentionally not a goal. | Go exporters default to secure endpoints and retry; `otlp4j` defaults to plaintext and no retry. Both are defensible, but users coming from Go will notice. |
| P1 | Validate metric data-point `flags` consistently and clarify HTTP retry timeout semantics. | `Span` and `LogRecord` validate flags at construction, but metric points do not; HTTP retries currently apply timeout per request attempt rather than across the whole retry operation. |
| P1 | Consider a public transformation helper layer for resource/scope traversal and copy-modify operations. | The built-in transforms hide nested reconstruction, but custom transforms expose OTLP hierarchy complexity quickly. |
| P2 | Add an advanced-client escape hatch only if real users need it. | Go exposes `WithGRPCConn`, `WithHTTPClient`, dial options, proxy, max request size, and service config. `otlp4j` intentionally has a smaller surface; keep it small unless extension pressure appears. |
| P2 | Add a short lifecycle cheat sheet and examples for count sinks, batchers, fan-out, and exporter facet ownership. | The lifecycle model is sound, but the mental overhead is one of the biggest API complexity points. |

## Public API Surface

The public surface is split across these modules:

| Module | Intended public role | Current notes |
| --- | --- | --- |
| `otlp4j-model` | Pure immutable OTLP domain records. | Exports `dev.nthings.otlp4j.model`; no transport or proto dependency. |
| `otlp4j-api` | Core sinks/sources, pipeline DSL, processors, connectors, receiver/exporter abstractions, config, and SPI. | Re-exports the model with `requires transitive dev.nthings.otlp4j.model`. |
| `otlp4j-transport-grpc` | `OtlpGrpcExporter` and `OtlpGrpcReceiver`. | Exports only `dev.nthings.otlp4j.transport.grpc`. |
| `otlp4j-transport-http` | `OtlpHttpExporter` and `OtlpHttpReceiver`. | Exports only `dev.nthings.otlp4j.transport.http`. |
| `otlp4j-codec` | Documented as internal shared model/proto mapping. | Currently exports `dev.nthings.otlp4j.codec` unqualified. |
| `otlp4j-proto` | Generated proto messages and services. | Qualified exports only to codec and transports. |

The day-one application imports are discoverable from `README.md` and `docs/public-api.md`: `OtlpGrpcReceiver`, `OtlpGrpcExporter`, `Pipeline`, `Transforms`, `BatchingProcessor`, `Connectors`, `TraceData`, `Span`, `Metric`, `LogRecord`, `ConsumeResult`, and the configuration records.

The design correctly keeps generated proto and gRPC types out of normal application code. The sample at `otlp4j-samples/src/main/java/dev/nthings/otlp4j/samples/OtlpE2eDemo.java` demonstrates that well: a complete receive-transform-fanout-export path is built without importing generated proto or gRPC stubs.

## Ergonomics

### Strengths

The transport entry points are clear. `OtlpGrpcExporter.to(host, port)`, `OtlpGrpcReceiver.on(host, port)`, `builder().ephemeralPort()`, and exporter-builder plus `ClientConfig.builder().fromEnvironment()` entry points make the common path short without hiding the transport choice.

The signal-specific sink aliases are useful. `TraceSink`, `MetricSink`, `LogSink`, and `ProfileSink` keep lambdas readable and avoid making users spell `Sink<TraceData>` everywhere.

`Sink.accepting(...)` and `Sink.fromStage(...)` remove boilerplate for the common "do work and accept unless an exception occurs" case. Without those adapters, returning `ConsumeResult.acceptedStage()` from every handler would make examples noisier.

`ConsumeResult` is a good abstraction for OTLP semantics. Modeling `Accepted`, `Partial`, and `Rejected` explicitly is better than overloading exceptions, booleans, or nullable return values. The generic type parameter also prevents accidental merging of trace, metric, log, and profile rejection counts.

The immutable records are safe for fan-out. `FanOut` can share the same batch reference across peers because model objects copy lists, maps, and byte arrays. That is a better default for Java gateway code than mutable proto wrappers.

The builders are conventional and readable. `Span.builder()`, `Metric.builder()`, `LogRecord.builder()`, and point builders are approachable for Java users and give good IDE completion.

### Friction Points

The immutable model lacks enough copy-modify APIs. `Attributes`, `ClientConfig`, and `ServerConfig` have `toBuilder()`, but builder-backed telemetry records such as `Span`, `Metric`, `LogRecord`, `NumberPoint`, `HistogramPoint`, `ExponentialHistogramPoint`, and `Exemplar` do not. `SummaryPoint` has an `of(...)` factory but no builder or copy helper. A gateway library is often used to enrich, redact, normalize, and filter telemetry. Those operations frequently require "same record except one field".

Custom resource/scope rewrites require manual nested reconstruction. The built-in `Transforms.withTracesResourceAttribute(...)` handles this internally, but callers writing their own transforms must construct `TraceData.ResourceSpans`, `TraceData.ScopeSpans`, `MetricsData.ResourceMetrics`, and similar wrappers by hand.

`ConsumeResult.rejected(String)` is semantically retryable because it has no cause, while `permanentRejected(String, Throwable)` is non-retryable. The named factories help, but the generic `rejected(...)` method is easy to pick accidentally. For a young API, consider steering users toward `retryableRejected(...)` and `permanentRejected(...)` in examples and docs, and making the retry semantics more prominent near the method list.

`Pipeline.Stage.to(Sink<T>)`, `Pipeline.Branch.fanOut(Sink<T>)`, and `FanOut.of(...)` are invariant in `T`, while `Source.subscribe(...)` and `BatchingProcessor.Builder.downstream(...)` accept `Sink<? super T>`. The current signatures are fine for final concrete signal types, but using `? super T` consistently would compose better with generic sinks.

`Connectors.spanCount(...)` and `Connectors.logRecordCount(...)` require `MetricSink`, not `Sink<? super MetricsData>`. That improves signal-specific readability but makes generic metric sinks slightly less convenient.

Each call to `exporter.traces()` returns a new facet object. This is acceptable because the facet delegates to the same exporter and shutdown is idempotent, but users may not expect a lifecycle-carrying view object to be recreated each time. The docs explain this well enough; keep examples assigning facets to locals only when the local is reused.

### Concrete Ergonomic Improvements

Add `toBuilder()` to the record types that already have builders, and add a builder or copy helper for `SummaryPoint`. This is the highest value small change for custom transforms.

Consider resource/scope traversal helpers, for example `TraceData.mapSpans(...)`, `TraceData.mapResourceSpans(...)`, or package-level transform builders. Keep the API narrow: the goal is to remove repetitive wrapper reconstruction, not to build a full query language.

Consider adding `withAttribute(...)` convenience methods to `Resource`, `InstrumentationScope`, and `Attributes`. `Attributes.toBuilder().put(...).build()` is already reasonable, but resource enrichment is common enough to justify a shorter path.

Add examples that show copy-modify transforms for spans and logs, not only resource attributes. This will reveal whether the model needs `toBuilder()` or traversal helpers before the API stabilizes.

## Modern Java Idioms

### Strengths

The project uses modern Java intentionally rather than cosmetically. Records are a good fit for immutable OTLP values. Sealed interfaces are a good fit for `AttributeValue`, `Metric.Data`, `ConsumeResult`, `Telemetry`, and TLS variants. Pattern matching over `Metric.Data` and `ConsumeResult` gives callers exhaustiveness checks.

JPMS boundaries are a major strength. `otlp4j-api` has no generated proto dependency, the model is a transitive API dependency, and transports are concrete modules. This is a clean Java-native counterpart to Go's package boundaries.

JDK `Flow.Publisher` is a reasonable choice for the receiver tap. It avoids adding a reactive-streams dependency while still giving demand-aware observation.

`CompletionStage` is an appropriate baseline abstraction for asynchronous consume/export operations. It keeps the API dependency-free and works with virtual threads, structured concurrency wrappers, and existing Java async code.

The build enforces Javadoc/doclint and keeps parameter names. That improves IDE and generated documentation quality.

### Risks

The JDK 25 requirement is a real adoption constraint. It enables Markdown doc comments and the newest language/runtime features, but many production Java libraries still target Java 17 or 21. If `otlp4j` is intended for broad embeddability, reassess whether JDK 25 is a hard requirement or a development preference.

Nullness is not applied consistently. `Metric`, `LogRecord`, `Exemplar`, `Transform`, and `ThrowingConsumer` are `@NullMarked`; other public model and API packages are not. Because `org.jspecify` is exposed transitively, this should be made systematic. Package-level `@NullMarked` plus targeted `@Nullable` on intentional nullable values would give users and tools clearer contracts.

`ServerConfig.serverExecutor` is intentionally nullable, and several builder fields default to null internally. Public nullability should be explicit rather than only described in prose.

Trace IDs and span IDs are represented as lowercase hex `String`s. This is easy to read and serialize, but less type-safe than dedicated `TraceId`/`SpanId` value records. The current constructors validate IDs, which mitigates the risk. If the API remains string-based, keep the validation strict and keep ID semantics documented near every builder method.

The API uses `long` for OTLP unsigned 32-bit flags. This is practical in Java, but a small `TraceFlags` or `SpanFlags` value type could make intent clearer if flag manipulation becomes common.

Metric data-point flags should get the same construction-time validation as spans and logs. `Span` and `LogRecord` normalize flags through `Ids.flags(...)`, but `NumberPoint`, `HistogramPoint`, `ExponentialHistogramPoint`, and `SummaryPoint` currently accept any `long`; `MetricsMapper` later casts those values to `int`, so out-of-range values can silently wrap at the transport boundary.

## Discoverability

### Strengths

`README.md` gets the high-level positioning right: `otlp4j` is an OTLP data-plane gateway/pipeline, not an application instrumentation SDK. This is essential because types named `Span`, `Metric`, and `LogRecord` can otherwise be mistaken for SDK instrumentation APIs.

`docs/public-api.md` is unusually useful. It maps important types to packages, explains OpenTelemetry Go concept mapping, warns about flattened accessor allocation, describes receive/export defaults, and gives lifecycle/shutdown guidance.

The concrete transport class names are easy to search for and hard to misinterpret. `OtlpGrpcExporter`, `OtlpGrpcReceiver`, `OtlpHttpExporter`, and `OtlpHttpReceiver` are clearer for Java users than a service-loader based transport selection layer would be.

Package-level Javadocs are concise and helpful. `core`, `pipeline`, `receiver`, `config`, and `spi` package comments tell users where to look next.

### Discoverability Problems

The codec module is discoverable as public even though docs say it is internal. `docs/public-api.md` says generated proto and codec modules are implementation details. `README.md` describes `otlp4j-codec` as internal. `otlp4j-codec/pom.xml` says its package is exported only to transports. But `otlp4j-codec/src/main/java/module-info.java` exports `dev.nthings.otlp4j.codec` unqualified. Users on the module path and classpath can see mapper classes and may treat them as supported.

There are stale documentation comments. `otlp4j-api/src/main/java/dev/nthings/otlp4j/receiver/Receiver.java` mentions "future `OtlpHttpReceiver`" even though it exists. `otlp4j-samples/src/test/java/dev/nthings/otlp4j/samples/OtlpE2eDemoTest.java` says the sample proves runtime transport discovery through a service-provider interface, while architecture docs explicitly say there is no provider discovery. `pom.xml` comments reference `docs/TEST_STRATEGY.md`, which does not currently exist under `docs/`. `docs/project.md` also describes the project as a "Java SDK", which conflicts with the README's careful non-instrumentation-SDK positioning.

The parent POM description says "A modular OpenTelemetry Protocol SDK", while the README says this is not the OpenTelemetry Java instrumentation SDK. The library may still be an SDK in the broad sense, but this wording can confuse users searching Maven Central or generated project metadata.

The public surface has many packages for a small library: `core`, `pipeline`, `receiver`, `exporter`, `processor`, `connector`, `config`, `spi`, and `model`. The docs mitigate this, but a first-time user still has to learn several nouns. This is acceptable if the project remains a gateway toolkit, but a one-page "Which package do I need?" table should remain prominent.

### Suggested Discoverability Improvements

Align the codec boundary before 1.0. If the export is only a JPMS build workaround, document it as unsupported in the module Javadoc and Maven description, and consider moving mappers behind an exported-to-qualified workaround or a non-published module. If codec is intentionally public, add a supported codec facade and stop calling it internal.

Update stale comments and metadata. These are small changes but matter because the docs currently carry much of the API's discoverability burden.

Add a "Start Here" section to `docs/public-api.md` with three code paths: receive and print, receive-transform-export, and construct-and-export a batch. The README already contains this information, but a compact task-oriented index would help.

Consider a future BOM or starter artifact once publishing starts. Requiring users to add both `otlp4j-api` and a transport is explicit and correct, but a Maven BOM would make version alignment easier.

## Composability

### Strengths

The API composes along signal boundaries. `Source<T>`, `Sink<T>`, `Transform<T>`, `FanOut<T>`, `Pipeline.Stage<T>`, and `BatchingProcessor<T>` provide a coherent typed graph model.

The pipeline DSL is small and predictable. `transform`, `filter`, `peek`, `branch`, `fanOut`, `join`, `to`, and `owns` cover common gateway patterns without introducing a full stream-processing framework.

Exporter facets compose well with pipelines. A terminal `exporter.traces()` is both a sink and a lifecycle-carrying object, so common pipelines do not need manual `owns(exporter)` calls.

The single-attachment rule on sources is clear and safer than silently multiplying consumers. It forces fan-out to be explicit, which matters for acknowledgements and partial-success merging.

The receiver tap is separate from the acknowledgement path. This separation is correct: observational subscribers should not accidentally affect OTLP delivery unless explicitly configured to block.

### Composability Friction

Custom transforms need too much structural knowledge. Any transform that changes a nested span, log record, metric point, resource, or scope currently needs to preserve resource/scope wrappers manually. This will discourage users from writing small reusable transforms.

`FanOut` documents that a peer that throws is captured as a per-peer `Rejected`, but the implementation catches only `RuntimeException` from synchronous `peer.consume(batch)` calls. A synchronous `Error` can escape and prevent later peers from receiving the batch. Either the docs should say only runtime exceptions are captured, or the implementation should catch `Throwable` consistently with `Pipeline.peek`'s fire-and-forget behavior.

`PipelineSubscription.shutdown(...)` uses one shared deadline across resources, but `forceFlush(...)` passes the same timeout to each owned resource sequentially. If multiple buffered resources exist, total flush time can exceed the requested timeout. This is a subtle public lifecycle behavior; either share the flush deadline too or document the difference.

Count connectors hide their downstream resource. The docs explain that a pipeline cannot see through a count sink to auto-own the exporter behind it, but this remains easy to get wrong. The sample avoids the issue by pointing the count sink at an exporter facet that is also used directly in the fan-out.

Profiles batching has a necessary sharp edge: distinct non-empty profile dictionaries cannot be merged losslessly and fail on flush. The warning in `docs/public-api.md` is strong. Keep `ProfilesData` experimental and avoid adding more profile manipulation APIs until the upstream profile model stabilizes.

### Suggested Composability Improvements

Add copy helpers before adding more processors. Reusable transformations become much easier if model records can be copied and changed fluently.

Consider a small `Transforms.mapSpans(...)` and `Transforms.mapLogRecords(...)` family. These would complement `keepSpansWhere(...)` and `keepLogRecordsWhere(...)` and cover common redaction/enrichment cases.

Consider `Transforms.mapResources(...)` for each signal, or a generic resource rewrite helper. Resource enrichment is one of the most common gateway operations.

Make the lifecycle semantics testable through examples. A short example that intentionally uses a count sink and requires `owns(exporter)` would teach the edge case better than prose alone.

## Documentation

### Strengths

The public API guide is detailed and honest. It explains allocation behavior, transport defaults, environment-variable precedence, production receiver hardening, lifecycle ownership, batching overflow, profile dictionary limits, and the difference between `Pipeline.peek` and `TelemetryTap`.

The architecture guide does a good job of explaining module boundaries and request flow. The diagrams and lifecycle descriptions make it much easier to understand why `ConsumeResult` exists and why fan-out merges partial successes by maximum rejected count instead of summing.

The examples are realistic enough to be useful. The end-to-end sample exercises two real OTLP/gRPC hops, resource enrichment, filtering, fan-out, and derived metrics without generated proto imports.

The docs repeatedly state that this is not the OpenTelemetry Java instrumentation SDK. That repetition is warranted.

### Documentation Gaps

The internal/public boundary is inconsistent for codec. This is the most important documentation issue because it changes users' perception of stability.

The Go comparison is helpful but should state the default differences more explicitly. A Go user may expect secure defaults, retry defaults, signal-specific environment-variable overrides, and `WithGRPCConn`/`WithHTTPClient` escape hatches.

There is no prominent "API stability" policy beyond the `0.1.0-SNAPSHOT` and `@Experimental` notes. Since this is pre-1.0, add a short statement about what may change and which modules/packages are considered supported.

Nullness and thread-safety are mostly implicit. The immutable model is thread-safe by construction, but lifecycle classes such as receivers, exporters, batchers, and subscriptions have concurrency behavior that should be summarized in public docs.

`docs/public-api.md` is long. It is valuable as a reference, but a shorter quick-start or cookbook page would improve first-use success.

### Documentation Cleanup List

Update `otlp4j-codec/pom.xml` and `otlp4j-codec/src/main/java/module-info.java` comments so they agree with the actual export behavior.

Update `Receiver.java` Javadoc to remove "future `OtlpHttpReceiver`".

Update `OtlpE2eDemoTest.java` Javadoc to remove the service-provider-interface discovery claim.

Update or remove the `docs/TEST_STRATEGY.md` reference in the parent POM comment, or add the missing document.

Consider changing the parent POM and `docs/project.md` descriptions from "SDK" to "OTLP gateway/pipeline library" or similar.

## Complexity

### Complexity That Pays For Itself

The immutable domain model adds construction cost but simplifies concurrent fan-out and avoids shared mutable proto state. This is a good tradeoff for Java gateway code.

`ConsumeResult` adds more states than a boolean or exception, but maps cleanly onto OTLP partial success and retry behavior. This complexity is justified.

Explicit transport entry points add compile-time dependencies, but make deployments predictable and avoid service-loader magic. This fits the module design.

The pipeline DSL is small enough to learn and does not try to become a general stream-processing engine.

### Complexity To Watch

Lifecycle ownership is the hardest part of the API. Exporter facets auto-own exporters, `BatchingProcessor` is auto-owned when directly attached, count sinks hide downstream owners, `owns(...)` is sometimes required, and shutdown order matters. The design is careful, but the mental model is non-trivial.

The receiver tap introduces a second delivery path with its own backpressure strategies. This is useful, but it increases the number of failure/drop modes users must understand.

The environment-variable rules are detailed because they intentionally support only general OTLP variables and not signal-specific overrides. The docs explain this, but the difference from official SDK exporters should be surfaced near every `fromEnvironment()` example.

Profiles are inherently complex and currently opaque. Keeping them behind `@Experimental` is correct.

The advanced transport SPI is public. Most users should not need `dev.nthings.otlp4j.spi`, so docs should continue framing it as an extension-author surface.

## Differences From Official OpenTelemetry Go APIs

### Correct Comparison Target

The closest Go analogue for `otlp4j` is not only the Go SDK exporter packages. It is a combination of Go SDK OTLP exporters and the OpenTelemetry Collector `pdata` APIs.

Official Go SDK exporter packages such as `otlptracegrpc`, `otlpmetricgrpc`, and `otlploghttp` plug into SDK providers (`TracerProvider`, `PeriodicReader`, `LoggerProvider`) and export instrumentation data. `otlp4j` is positioned as a receive/process/route/forward gateway for already-materialized OTLP batches.

Collector `pdata` packages such as `ptrace`, `pmetric`, `plog`, `pcommon`, and `pmetricotlp` are closer to `otlp4j-model` plus the transport request/response layer.

### API Shape

| Area | Official Go shape | `otlp4j` shape | Feedback |
| --- | --- | --- | --- |
| Exporters | One package per signal and transport, for example `otlptracegrpc`, `otlpmetricgrpc`, `otlploghttp`. | One exporter per transport with signal facets: `exporter.traces()`, `metrics()`, `logs()`, `profiles()`. | Good gateway-oriented Java design. Keep documenting it for Go users. |
| Options/config | Functional options such as `WithEndpoint`, `WithTimeout`, `WithInsecure`, `WithRetry`. | Java builders and config records. | Idiomatic Java. No need to mimic functional options. |
| Per-call context | `Export(ctx, ...)`, `Shutdown(ctx)`, `ForceFlush(ctx)`. | `CompletionStage` for consume/export and `Duration` for shutdown/flush deadlines. | Reasonable Java translation. Lack of per-call context is an intentional difference. |
| Data model | Collector `pdata` mutable reference wrappers with `AppendEmpty`, `CopyTo`, `MoveTo`, `EnsureCapacity`, `RemoveIf`, and `Sort`. | Immutable records with builders and copied collections. | Safer for fan-out; less convenient for in-place collector-style mutation. Add copy helpers, not mutability. |
| Resource/scope grouping | `ResourceSpans`, `ScopeSpans`, `ResourceMetrics`, `ScopeMetrics`, etc. | Same hierarchy via records under `TraceData`, `MetricsData`, `LogsData`, `ProfilesData`. | Strong alignment. |
| Codec | Collector exposes `ProtoMarshaler`, `ProtoUnmarshaler`, `JSONMarshaler`, `JSONUnmarshaler`. OTLP request wrappers expose `MarshalProto`, `MarshalJSON`, `UnmarshalProto`, `UnmarshalJSON`. | Codec is intended internal; transports expose binary protobuf only. | Good for simple users, but a gap for Collector-style interop. Decide deliberately. |
| Request/response wrappers | Packages like `pmetricotlp` expose `ExportRequest`, `ExportResponse`, partial success, and gRPC server/client wrappers. | `TraceData`/`MetricsData`/`LogsData` act as domain request equivalents; `ConsumeResult` maps to responses inside transports. | Higher-level and cleaner, but less useful for low-level interop. |
| Metrics SDK knobs | Go metric exporters expose temporality and aggregation selectors. | `otlp4j` works with already-materialized OTLP metrics. | Do not add these unless `otlp4j` becomes an instrumentation SDK exporter. |
| Profiles | Go Collector has richer profile `pdata` APIs; profile support is still newer. | `ProfilesData` is `@Experimental` and opaque for profile payloads. | Reasonable. Keep experimental. |

### Defaults And Environment Variables

Official Go gRPC exporters document secure defaults such as `https://localhost:4317`. Official Go HTTP exporters document `https://localhost:4318` and signal paths such as `/v1/logs`. `otlp4j` defaults to plaintext `localhost:4317` for gRPC exporters, plaintext `localhost:4318` for HTTP exporters, and plaintext `0.0.0.0` for receivers.

Official Go exporters generally enable a default retry policy for transient retryable errors: retry after 5 seconds with exponential backoff for up to about 1 minute unless overridden. `otlp4j` defaults to `RetryPolicy.none()`.

The HTTP retry timeout semantics also differ from Go's documented `WithTimeout` behavior. `HttpOtlpClient` builds each `HttpRequest` with `config.timeout()` and then sleeps between retry attempts, so the total wall-clock time for one export can exceed the configured timeout when retries are enabled. Go documents exporter timeout as bounding the export attempt across retry. `otlp4j` should either adopt a shared deadline across HTTP retries or document the per-attempt behavior explicitly.

Official Go exporters support signal-specific environment-variable overrides such as `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_METRICS_TIMEOUT`, and `OTEL_EXPORTER_OTLP_LOGS_HEADERS`. `otlp4j` currently reads only general `OTEL_EXPORTER_OTLP_*` variables and explicitly documents signal-specific overrides as out of scope because one exporter can drive all four signals over one connection.

Official Go HTTP exporters provide both base endpoint and signal-specific URL/path behavior. `otlp4j`'s HTTP exporter supports a path prefix and standard per-signal paths, which is a good Java gateway compromise.

These differences are not automatically wrong. They should be documented as deliberate. If `otlp4j` wants to feel familiar to users coming from Go or official SDKs, the two largest parity gaps are secure/retry defaults and signal-specific environment variables.

### Advanced Transport Options

Official Go exposes advanced transport hooks:

| Go option | Current `otlp4j` equivalent | Recommendation |
| --- | --- | --- |
| `WithGRPCConn` | None; implement SPI/custom transport. | Do not add unless users need connection reuse or channel injection. |
| `WithDialOption` / `WithServiceConfig` | Partial retry service config internally for gRPC. | Keep hidden unless advanced gRPC users ask. |
| `WithHTTPClient` | None; HTTP transport owns JDK `HttpClient`. | Consider only if proxy/TLS/client customization becomes a real need. |
| `WithProxy` | JDK default proxy behavior depends on client setup; no public knob. | Document current behavior or add later with HTTP client customization. |
| `WithTLSClientConfig` | `Tls.systemTrust()`, `Tls.trust(path)`, `Tls.custom(...)`. | Java path-based TLS API is simpler; keep it. |
| `WithMaxRequestSize` | Receiver inbound size cap exists; exporter serialized request size cap does not. | Consider exporter-side cap if abuse/resource protection is a goal. |
| `WithReconnectionPeriod` | None. | Avoid unless there is a concrete retry/reconnect requirement. |

The smaller `otlp4j` option set is an advantage for most users. The API should not import all Go knobs by default.

### Model Differences

Go Collector `pdata` is optimized for collector internals: mutable wrappers, zero-copy-ish movement, append-empty construction, explicit capacity management, and public marshaling. `otlp4j` is optimized for safe Java application embedding: immutable records, builders, and no generated proto exposure.

The immutable Java model is the right default for this project. The cost is ergonomics for mutation-heavy processors. Solve that with `toBuilder()` and mapping helpers, not by exposing mutable state.

Go uses typed IDs and timestamps through `pcommon.TraceID`, `pcommon.SpanID`, and `pcommon.Timestamp`. `otlp4j` uses hex strings and `long` epoch nanoseconds. This keeps construction simple but loses some type safety. If stronger typing is added later, do it before 1.0 because ID and timestamp types permeate the model.

Go exposes OTLP/JSON marshal/unmarshal. `otlp4j` explicitly supports binary protobuf over HTTP only and no OTLP/JSON. This is fine for a focused transport library, but users building test fixtures, files, or HTTP/JSON interop will ask for codecs eventually.

## Recommended API Evolution Path

### Before Wider Use

Resolve the codec public/private contradiction.

Add `toBuilder()` to primary model records.

Validate metric data-point `flags` at construction and decide whether HTTP retry timeout should be per-attempt or end-to-end.

Make nullness systematic with `@NullMarked` and `@Nullable`.

Fix stale docs and metadata.

Clarify default differences from official Go and OpenTelemetry SDK exporters.

### Before 1.0

Decide whether `String` IDs and `long` flags/timestamps are final.

Decide whether any public codec/request facade is in scope.

Decide whether secure/retry defaults should match official SDK defaults or remain local-friendly.

Stabilize the transform helper vocabulary.

Add an explicit API stability policy by module/package.

### Defer Until Requested

Do not add metric temporality or aggregation selector APIs unless `otlp4j` becomes an SDK exporter.

Do not add every Go transport option preemptively.

Do not make the model mutable to match Collector `pdata`.

Do not add service-loader transport discovery unless there is a real plugin/distribution use case.

## Source Notes

Local files reviewed include:

| Topic | Files |
| --- | --- |
| Module boundaries | `README.md`, `docs/architecture.md`, all `module-info.java` files |
| Public API guide | `docs/public-api.md` |
| Model | `TraceData.java`, `MetricsData.java`, `LogsData.java`, `Span.java`, `Metric.java`, `LogRecord.java`, `Attributes.java`, `AttributeValue.java`, `ConsumeResult.java` |
| Core/pipeline | `Sink.java`, `Source.java`, `Subscription.java`, `Pipeline.java`, `FanOut.java`, `Transform.java` |
| Processors/connectors | `Transforms.java`, `BatchingProcessor.java`, `Connectors.java` |
| Config | `ClientConfig.java`, `ServerConfig.java`, `Tls.java`, `RetryPolicy.java`, `OtlpEnv.java` |
| Transports | `OtlpGrpcExporter.java`, `OtlpGrpcReceiver.java`, `OtlpHttpExporter.java`, `OtlpHttpReceiver.java`, `AbstractOtlpExporter.java`, `AbstractOtlpReceiver.java` |
| Samples | `otlp4j-samples/src/main/java/dev/nthings/otlp4j/samples/OtlpE2eDemo.java` |

Official Go references consulted:

| Area | URL |
| --- | --- |
| Trace gRPC exporter | https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc |
| Metric gRPC exporter | https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc |
| Log HTTP exporter | https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp |
| Collector traces pdata | https://pkg.go.dev/go.opentelemetry.io/collector/pdata/ptrace |
| Collector common pdata | https://pkg.go.dev/go.opentelemetry.io/collector/pdata/pcommon |
| Collector metric OTLP wrappers | https://pkg.go.dev/go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp |
