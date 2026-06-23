# OTLP4J API Enhancement Proposals

Enhancement proposals that are factual, actionable, and worth considering before API stabilization.

## Immediate

### 1. Complete construction-time model validation

Several public model records still accept invalid or null fields that are dereferenced later by the codec/export path. For example, `LogRecord` validates ids and flags but not `severity`, `severityText`, `body`, `attributes`, or `eventName`, while `LogsMapper` dereferences those values during encoding. `Metric` null-checks `data` but not `name`, `description`, `unit`, or `metadata`, while `MetricsMapper` dereferences them. `Exemplar` documents trace/span id expectations but does not validate them in its constructor.

Move required model invariants to construction time so invalid data fails before async export. Keep deliberately nullable fields documented, such as `Exemplar.value` for unset wire oneofs, but make all required fields explicit with `Objects.requireNonNull` or normalization.

### 2. Define transform null semantics

`Pipeline` currently uses `null` as its internal filter/drop sentinel. `Stage.transform(...)` wraps user transforms with a null guard, but if a user `Transform<T>` accidentally returns `null`, the terminal is skipped and the batch is acknowledged as accepted. `Transform.apply` does not document `null` as a drop signal.

Make null behavior explicit before 1.0. Prefer treating a null transform result as a programming error, e.g. reject/throw with a clear message, or introduce an internal sentinel that cannot be confused with user data. Silent accepted drops are the risky behavior to avoid.

### 3. Fix public documentation drift

Several docs describe an older API surface. `docs/public-api.md` still refers to `Consumer`, `TraceConsumer`, `ClientTransportConfig`, `ServerTransportConfig`, transport providers, and `Protocol`; the current public API uses `Sink`, `TraceSink`, `ClientConfig`, `ServerConfig`, and direct transport classes. `docs/architecture.md` also says `Metric.data()` can be `null`, while the current model uses the non-null `Metric.NoData` variant.

The drift is not limited to `docs/`: the README and sample docs still reference an aggregate/runtime `otlp4j-transport` artifact or ServiceLoader-style provider story, while the current modules expose separate `otlp4j-transport-grpc` and `otlp4j-transport-http` entry points.

Update README, `docs/public-api.md`, `docs/architecture.md`, sample docs, and package/POM metadata so the documented import paths, artifact names, module names, and type names match the current exported API.

### 4. Clarify positioning versus the OpenTelemetry Java SDK

The codebase provides immutable OTLP data records, receivers, exporters, transforms, batching, connectors, and tap streams. It does not provide tracer/meter/logger APIs, span lifecycle, `SpanContext`, context propagation, resource detectors, or metric instruments. The existing `Span`, `Metric`, and `LogRecord` builders are batch/model construction helpers, not an application instrumentation SDK.

Document this explicitly in the README and public API guide:

- otlp4j is an OTLP gateway/pipeline library for receiving, processing, observing, and forwarding OTLP data.
- Application instrumentation should use the official OpenTelemetry Java API/SDK.
- otlp4j can complement that SDK when telemetry needs to be routed through an embedded JVM gateway.

### 5. Make exporter facet ownership hard to misuse

`AbstractOtlpExporter.traces()`, `.metrics()`, `.logs()`, and `.profiles()` return method-reference facets backed by the internal `OtlpClient`. `Pipeline` can auto-collect lifecycle only from terminals that directly implement `AutoCloseable` or from `FanOut` peers, so `.to(exporter.traces())` does not transfer exporter ownership. The current safe patterns are `.to(exporter.traces(), exporter)` or `.owns(exporter).to(exporter.traces())`.

Keep the explicit owner overload for now, but make the common path safe by construction. For example, return facet objects that implement both the signal sink and lifecycle delegation to the exporter, or introduce an `OwnedSink<T>`/terminal wrapper that `Pipeline` can recognize automatically. The goal is for `.to(exporter.traces())` to drain/flush the exporter without requiring users to remember a second argument.

### 6. Add simple sink authoring adapters

`ConsumeResult<T>` is justified because OTLP partial-success and whole-batch rejection semantics are real protocol behavior. The friction is that simple users must still return `CompletionStage<ConsumeResult<T>>` and usually write `ConsumeResult.acceptedStage()`.

Add small adapters instead of weakening the core contract, such as:

- `Sink.accepting(ThrowingConsumer<T>)`, accepting on successful return and mapping thrown exceptions to rejection/failure.
- `Sink.fromStage(Function<T, CompletionStage<Void>>)` for async work that only needs success/failure.
- Signal-specific convenience factories if keeping `TraceSink`, `MetricSink`, `LogSink`, and `ProfileSink` as public SAM aliases.

### 7. Normalize owned blocking and thread creation

The project targets JDK 25 and already uses virtual threads in the tap and transports. A few owned executors still create daemon platform threads with `new Thread(...); setDaemon(true)` in `BatchingProcessor` and `AbstractOtlpExporter`.

Use the modern thread builder API consistently, e.g. `Thread.ofPlatform().daemon().name(...).factory()` for single-thread owned executors. Consider virtual threads where the executor is used only for blocking transport close/drain work and does not need a long-lived platform thread.

Also avoid blocking shutdown waits on the common pool. `GrpcOtlpServer.waitForTermination(...)` and `HttpOtlpServer.stop(...)` run blocking termination work through `CompletableFuture.runAsync(...)` without an explicit executor. Give those waits a named executor or virtual-thread task so server shutdown does not consume common-pool workers.

## Before API Stabilization

### 8. Add allocation-free traversal and count helpers

`TraceData.spans()`, `MetricsData.metrics()`, `LogsData.logRecords()`, and `ProfilesData.profiles()` each traverse the resource/scope hierarchy and allocate a fresh list on every call. The docs warn about this. Some internal code avoids flattening by walking the hierarchy directly, such as the count connectors, but other paths still use flattened accessors for counts.

Add allocation-free helpers for common traversal:

- `TraceData.forEachSpan(java.util.function.Consumer<? super Span>)`
- `MetricsData.forEachMetric(java.util.function.Consumer<? super Metric>)`
- `LogsData.forEachLogRecord(java.util.function.Consumer<? super LogRecord>)`
- `ProfilesData.forEachProfile(java.util.function.Consumer<? super ProfilesData.Profile>)`

Also consider signal item-count helpers for the protocol counts used by batching, partial-success reporting, and connectors.

### 9. Revisit connector public surface and lifecycle semantics

`CountConnector` is already package-private, but `Connector<I, O>`, `Connectors`, and `FailurePolicy` are public. Today the package exposes only two concrete derivations: trace-to-span-count metrics and log-to-log-record-count metrics. The abstraction is useful if signal-changing derivation is meant to be a general extension point, but oversized if the intended API is only the two count helpers.

The current lifecycle story is also ambiguous. `Connector.downstream()` exposes the downstream sink, and public docs describe connectors as owning downstream lifecycle, but `Pipeline` does not auto-discover connector downstream resources. Shutdown docs therefore require connector downstream owners to be registered separately.

Before 1.0, choose one direction:

- Keep a general connector API, document custom connector authoring, and make lifecycle ownership explicit.
- Teach `Pipeline` or connector implementations to collect/delegate downstream lifecycle when that is the intended ownership model.
- Or collapse the public API to simpler factory methods returning `Sink<TraceData>` / `Sink<LogsData>` count sinks, keeping the implementation package-private.

### 10. Finish or explicitly scope OTLP environment and endpoint support

`ClientConfig.Builder.fromEnvironment()` and the transport builders read only the general `OTEL_EXPORTER_OTLP_*` variables: endpoint, headers, timeout, compression, certificate, client certificate, and client key. Signal-specific overrides and `OTEL_EXPORTER_OTLP_INSECURE` are intentionally not read today.

The current endpoint support also ignores HTTP path prefixes. `ClientConfig` stores only host/port/TLS-related settings, and `HttpOtlpClient` always appends fixed standard signal paths to `scheme://host:port`. Certificate environment variables are considered only while applying an `https` endpoint, not as independent builder inputs.

Keep environment loading opt-in for deterministic construction, but improve discoverability with static transport factories such as `OtlpGrpcExporter.fromEnvironment()` and `OtlpHttpExporter.fromEnvironment()`. Before 1.0, either implement the missing spec behavior or label the current feature clearly as support for the general-variable subset only, with endpoint path prefixes and signal-specific overrides out of scope.

### 11. Align bulk header semantics

`ClientConfig.Builder.headers(Map<String, String>)` clears existing headers before adding the supplied map. The transport builder Javadocs describe their `headers(...)` methods as adding all headers on top of any already set.

Choose replace or merge semantics for `.headers(map)`, then align `ClientConfig`, transport builders, Javadocs, and `docs/public-api.md`. If both behaviors are useful, expose them with distinct names such as `headers(...)` for replace and `addHeaders(...)` for merge.

### 12. Hide pipeline implementation classes from public docs

`Pipeline.Stage` and `Pipeline.Branch` are public sealed nested interfaces whose permitted implementations are package-private nested classes (`StageImpl`, `BranchImpl`). Users cannot use those implementation classes, but generated Javadocs may still expose them through the sealed permits list.

Verify the generated Javadoc output. If it exposes implementation names prominently, make the implementation classes private if the compiler and Javadoc output allow it, or adjust the design so public docs show only user-facing pipeline concepts.
