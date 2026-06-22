# API Feedback Report

Date: 2026-06-22

Scope: public APIs exported by `otlp4j-api` and `otlp4j-model`, README/docs/samples, and the bundled gRPC transport where it affects public semantics. Upstream comparator: official OpenTelemetry Go SDK OTLP exporters and OpenTelemetry Collector component/consumer APIs.

## Executive Summary

`otlp4j` has a coherent and promising public API. The strongest choices are the proto-free immutable model, explicit JPMS module boundaries, signal-specific consumer aliases, a small receiver/exporter facade, and a pipeline DSL that covers filtering, transforms, fan-out, batching, connectors, and taps without forcing users into generated protobuf or gRPC types.

Compared with the official Go APIs, `otlp4j` intentionally sits closer to a small Collector-style gateway library than a direct SDK exporter package. That is fine, but users coming from Go will expect signal/transport-specific discoverability, context-aware cancellation/error semantics, secure defaults or explicit insecure defaults, HTTP/gRPC separation, and clear ownership rules for injected channels/clients. The report below recommends which differences to preserve and which to close.

## Public Surface Reviewed

The exported JPMS surface is intentionally small at the module level:

| Module | Exported packages | Notes |
| --- | --- | --- |
| `dev.nthings.otlp4j.model` | `dev.nthings.otlp4j.model` | Pure immutable OTLP domain records, no generated proto dependency. |
| `dev.nthings.otlp4j.api` | `pipeline`, `receiver`, `exporter`, `processor`, `connector`, `spi` | Re-exports model transitively and discovers transport providers through `ServiceLoader`. |
| `dev.nthings.otlp4j.transport` | none | Provides gRPC client/server implementations through JPMS and classpath service files. |

The main user-facing types are:

| Area | Types |
| --- | --- |
| Model | `TraceData`, `MetricsData`, `LogsData`, `ProfilesData`, `Span`, `Metric`, `LogRecord`, `Attributes`, `AttributeValue`, points/exemplars/resources/scopes. |
| Pipeline | `Pipeline`, `Source`, `Consumer`, signal aliases, `Transform`, `FanOut`, `ConsumeResult`, `Subscription`, `Telemetry`, `Drainable`, `Flushable`. |
| Receiver | `OtlpGrpcReceiver`, `Receiver`, `TelemetryTap`, `TapOptions`, `BackpressureStrategy`. |
| Exporter | `OtlpGrpcExporter`, `Exporter`. |
| Processing | `Transforms`, `BatchingProcessor`, `DropPolicy`. |
| Connectors | `Connector`, `Connectors`, `FailurePolicy`. |
| SPI | `OtlpClient`, `OtlpServer`, providers, transport config records, `Tls`, `Compression`, `RetryPolicy`. |

Classpath users can still see public classes under non-exported `internal` packages such as `dev.nthings.otlp4j.receiver.internal` and `dev.nthings.otlp4j.api.internal`. JPMS hides them, but a classpath user or IDE search can discover them. Before publishing a stable artifact, consider making internal top-level classes package-private where possible or moving them to a clearly non-consumable artifact/package convention.

## Scorecard

| Dimension | Assessment | Main reason |
| --- | --- | --- |
| Ergonomics | Good | Simple happy path and builders cover the common flows. |
| Discoverability | Good | README and `docs/public-api.md` name the main entry points clearly; exported package structure is understandable. |
| Composability | Good for per-signal flows, moderate for cross-signal graphs | Pipeline/filter/transform/fan-out/batch/tap compose well; connectors are terminal consumers rather than graph stages. |
| Documentation | Strong for a pre-1.0 API | Public guide is detailed; a full tap subscriber example is still missing. |
| Complexity | Moderate | The API hides protobuf/gRPC complexity, but introduces pipeline ownership, partial-success semantics, async stages, batching, taps, and SPI concepts. |
| Go API alignment | Partial and intentionally different | Strong on signal-specific consumers and OTLP partial success; missing context/error shape, HTTP split, secure defaults, and Go's package-level discoverability. |

## Ergonomics

Strengths:

- The entry point set is easy to remember: `OtlpGrpcReceiver`, `OtlpGrpcExporter`, `Pipeline`, `Transforms`, and `BatchingProcessor` cover the common path.
- The happy path is concise and Java-native: `OtlpGrpcReceiver.on(...).start()`, `OtlpGrpcExporter.to(...)`, and `Pipeline.from(receiver.traces()).transform(...).to(exporter.traces())` read naturally.
- Signal-specific SAM interfaces such as `TraceConsumer`, `MetricConsumer`, `LogConsumer`, and `ProfileConsumer` make lambdas readable and avoid callers having to spell generic `Consumer<TraceData>` everywhere.
- `ConsumeResult.acceptedStage()` removes the most common `CompletionStage` boilerplate.
- Immutable model records are safe to share through fan-out without requiring Collector-style mutation capabilities.
- Builders for `Span`, `Metric`, `LogRecord`, `Attributes`, transport configs, receiver, exporter, and batcher reduce constructor noise for common paths.

Friction points:

- The receiver and exporter convenience names are asymmetric. `OtlpGrpcExporter.to(host, port)` builds a ready-to-use exporter, while `OtlpGrpcReceiver.on(host, port)` builds but does not start. The comments say this, but the method names do not communicate lifecycle state equally well.
- A receiver source has exactly one attachment slot. This is a reasonable design, but users may expect multiple subscriptions to work because the type is named `Source`. The required `branch().fanOut(...).join()` pattern should be prominent in every receiver example that mentions multiple consumers.
- `Metric.data()` being nullable is a sharp Java edge. It preserves OTLP `DATA_NOT_SET`, but it weakens the otherwise strong typed model. A sealed `Metric.Unset` data variant or `Optional<Metric.Data>` would make the exceptional state explicit.
- `CompletionStage<ConsumeResult<T>>` is correct for async delivery, but the API currently has no context object for request deadline, cancellation, metadata, peer identity, or retry intent. That keeps the API simple, but it means advanced receivers cannot make decisions with the same information Go consumers receive through `context.Context`.

Recommended ergonomic changes:

| Priority | Recommendation |
| --- | --- |
| P1 | Consider `Metric.Unset` or `Optional<Metric.Data>` before stabilizing the model API, so the `DATA_NOT_SET` state is explicit in the type rather than only in a nullable accessor. |

## Discoverability

Strengths:

- The README has a clear "Entry points" section and an executable end-to-end sample.
- `docs/public-api.md` provides a useful type-to-package table, domain model summary, receiver/exporter examples, pipeline routing table, batching docs, connector docs, tap docs, SPI docs, and shutdown order.
- JPMS exports make the intended public surface understandable for module-path users.
- Class names include the transport for the concrete transport types: `OtlpGrpcReceiver` and `OtlpGrpcExporter`.

Friction points:

- The public `spi` package is exported next to user-facing packages. This is valid for extension authors, but application users may discover `OtlpClientProvider`, `OtlpServerProvider`, `ClientTransportConfig`, and `ServerTransportConfig` before they understand the simpler receiver/exporter facade.
- Official Go discoverability is package-name driven: `otlptracegrpc`, `otlpmetricgrpc`, `otlploggrpc`, `otlphttp`, Collector `consumer`, Collector `component`. `otlp4j` relies more on a documentation table and class names. That is idiomatic for Java, but the docs should keep mapping "signal + transport + role" explicitly.
- Generated proto and transport internals are hidden by modules, which is good, but public classes in `internal` packages still appear on the classpath.

Recommended discoverability changes:

| Priority | Recommendation |
| --- | --- |
| P1 | Add a short "If you know OpenTelemetry Go" or "Concept map" section mapping Go packages/concepts to `otlp4j` concepts. |
| P2 | Split SPI documentation into an "extension authors" section so application users can ignore it until needed. |
| P2 | Make classpath-visible internal classes package-private where feasible, or add a stronger internal naming convention. |

## Composability

Strengths:

- `Pipeline.from(source)` keeps the simple path linear and readable.
- `Transform<T>` deliberately stays `T -> T`, which avoids hidden signal changes in normal processing.
- `FanOut` concurrent peer delivery and `ConsumeResult.fanOutMerge` are well designed for immutable batches. Worst-case partial rejection, rather than summing peers, matches the fact that every peer saw the same input.
- `BatchingProcessor<T>` composes as a terminal consumer and implements `Drainable`/`Flushable`, so directly attached processors participate in subscription lifecycle.
- `Connectors.spanCount` and `Connectors.logRecordCount` prove cross-signal derivation can be expressed without bloating the base pipeline type.
- `TelemetryTap` gives live observation without tying the pipeline to a third-party reactive library.

Friction points:

- Cross-signal connectors are consumers rather than pipeline stages. This is simple, but it prevents fluent shapes such as `Pipeline.from(receiver.traces()).connect(...).to(exporter.metrics())`. The current form works best inside `branch().fanOut(connector)`.
- Ownership registration happens at the stage level, not per branch peer. Users need to understand that hidden resources behind any peer must be registered before `branch()`/`join()`.
- `Pipeline.peek` is synchronous and swallows all throwables. That is good for non-disruptive observation, but users may confuse it with a real processing stage or an async observer. The docs correctly point to `TelemetryTap`; examples should reinforce that distinction.
- `TelemetryTap.setOptions(...)` applies to future subscriptions only. This is documented, but it is a subtle composability rule for Flow users.
- The immutable model eliminates mutability hazards, but flattened accessors such as `spans()`, `metrics()`, and `logRecords()` allocate a new flattened list each call. Connectors already avoid this for counts; user docs should mention it for hot paths.

Recommended composability changes:

| Priority | Recommendation |
| --- | --- |
| P1 | Add branch examples with multiple owned resources, not just one exporter. |
| P1 | Consider a connector stage API only if more built-in connectors appear. Do not add it prematurely; the current consumer-based connector is minimal and adequate for two count connectors. |
| P2 | Add a `TelemetryTap` complete subscriber example using `Flow.Subscriber`, demand, cancellation, and backpressure options. |
| P2 | Document flattened accessor allocation costs in the model section. |

## Documentation

The existing documentation is better than typical pre-release libraries. The README and `docs/public-api.md` are practical, the architecture doc explains module boundaries and request flow, and the sample tests the public API through two real gRPC hops.

Remaining documentation additions:

| Priority | Issue | Current state | Needed change |
| --- | --- | --- | --- |
| P2 | Tap usage | Docs show publishers but not a full subscriber. | Add a minimal `Flow.Subscriber` example. |

## Complexity

The API removes low-level wire complexity but still requires users to understand several concepts:

- Four signals with separate sources and consumers.
- Immutable OTLP grouping: resource, scope, item, plus flattened accessors.
- Async `CompletionStage` results and item-level partial success.
- Whole-batch rejection versus partial rejection versus exceptional completion.
- Receiver lifecycle, subscription lifecycle, exporter lifecycle, and ownership transfer.
- Batching queue capacity, max batch size, max age, drop policy, and flush/shutdown semantics.
- Tap backpressure strategies separate from the acknowledgement path.
- Transport SPI and ServiceLoader resolution.

This is acceptable complexity for a gateway/collector-style library, but it should be staged in docs and APIs:

- The simple path should need no SPI knowledge.
- Partial-success and retry semantics should be described once in precise language and linked from receiver, exporter, and batching docs.

## Upstream Official Go API Differences

This section compares against the official OpenTelemetry Go SDK OTLP exporters and OpenTelemetry Collector APIs.

### Package and Type Shape

Go SDK exporters are signal-specific and transport-specific packages:

| Go package | Role |
| --- | --- |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` | Trace OTLP/gRPC SDK exporter. |
| `go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc` | Metric OTLP/gRPC SDK exporter. |
| `go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc` | Log OTLP/gRPC SDK exporter. |
| `go.opentelemetry.io/proto/otlp/...` | Generated raw OTLP protobuf API. |
| `go.opentelemetry.io/collector/consumer` | Collector signal consumers. |
| `go.opentelemetry.io/collector/component` | Collector lifecycle and factories. |

`otlp4j` instead exposes one `OtlpGrpcExporter` and one `OtlpGrpcReceiver`, each with per-signal facets. This is ergonomic for Java and avoids many small classes, but it hides the signal/transport split that Go makes discoverable through import paths.

Recommendation: keep the unified Java facade, but preserve signal/transport discoverability in names and docs. If HTTP support is added, prefer explicit `OtlpHttpExporter` and `OtlpHttpReceiver` rather than overloading gRPC config objects.

### Construction and Options

Go uses functional options:

```go
exp, err := otlptracegrpc.New(ctx,
    otlptracegrpc.WithEndpoint("collector:4317"),
    otlptracegrpc.WithInsecure(),
    otlptracegrpc.WithTimeout(10*time.Second),
)
```

`otlp4j` uses builders and config records:

```java
var exporter = OtlpGrpcExporter.builder()
        .endpoint("collector", 4317)
        .timeout(Duration.ofSeconds(10))
        .build();
```

The Java builder style is appropriate. Go exposes endpoint, endpoint URL, insecure, TLS credentials, headers, compression, retry, timeout, max request size, reconnection period, service config, and injected gRPC connection directly in the transport package; `otlp4j` exposes endpoint, timeout, TLS, headers, compression, and retry directly on `OtlpGrpcExporter.Builder` (and the common server knobs on `OtlpGrpcReceiver.Builder`), with `transport(config)` for full replacement and the less common knobs left to the config record.

### Context, Cancellation, and Errors

Go methods receive `context.Context` and return `error`:

- Exporter construction: `New(ctx, ...Option) (*Exporter, error)`.
- Export: `Export(ctx, data) error`.
- Shutdown and flush: `Shutdown(ctx) error`, `ForceFlush(ctx) error`.
- Collector lifecycle: `Start(ctx, host) error`, `Shutdown(ctx) error`.
- Collector consumers: `ConsumeTraces(ctx, td) error`, `ConsumeMetrics(ctx, md) error`, `ConsumeLogs(ctx, ld) error`.

`otlp4j` uses `CompletionStage<ConsumeResult<T>>` for delivery and `shutdown(Duration)`/`forceFlush(Duration)` for lifecycle. This is natural in Java and preserves OTLP partial-success data better than a plain exception. The missing piece is request context. Consumers cannot inspect cancellation, deadlines, headers, remote peer info, or a retry hint.

Recommendation: do not add context to every API unless needed, but document this deliberate difference. If request metadata becomes important, add a parallel contextual interface such as `ContextualConsumer<T>` rather than breaking the simple `Consumer<T>` SAM.

### Defaults and Security

Go gRPC exporters document secure transport as the default, with `WithInsecure()` to opt out. They default to `https://localhost:4317` in package docs, while also supporting endpoint schemes and env vars.

`otlp4j` defaults to plaintext `localhost:4317` for exporters and plaintext `0.0.0.0:4317` for receivers. That is convenient for local collector-style development, but it is a notable difference from SDK exporter expectations and a production-security hazard if users expose receivers accidentally.

Recommendation: if plaintext defaults are retained, keep the README warning prominent and add a "production receiver checklist" covering bind host, TLS, executor, inbound size cap, concurrency cap, and auth story. If the API stabilizes for general use, consider secure-by-default clients or named constructors such as `toInsecure(...)` versus `to(...)`.

### HTTP and gRPC Split

Go exposes gRPC and HTTP exporters as distinct packages, and the Collector OTLP receiver supports both protocols. The Collector HTTP receiver has signal paths such as `/v1/traces`, `/v1/metrics`, `/v1/logs`, and `/v1/profiles`, plus CORS and HTTP/TLS/auth config.

`otlp4j` currently exposes only gRPC transport. That is fine for the current capability set, but documentation should say "OTLP/gRPC only" whenever comparing to OTLP generally.

Recommendation: when HTTP arrives, do not force HTTP concepts into `ClientTransportConfig`. Use separate `OtlpHttpExporter`/`OtlpHttpReceiver` and HTTP-specific config for URL paths, proxy, CORS, encoding, and HTTP client injection.

### Batching, Retry, and Queueing

Go SDK exporters do not own all batching behavior:

- Traces use SDK batch span processors.
- Metrics use periodic readers.
- Logs use batch processors.
- Collector exporters often use `exporterhelper` for queueing, retry, timeout, and persistent queues.

`otlp4j` provides `BatchingProcessor<T>` as a pipeline component and `RetryPolicy` as a gRPC transport setting. This separation is good. Users still need docs explaining that exporter `forceFlush` is currently a no-op because the exporter has no buffer, while batcher `forceFlush` drains queued telemetry.

Recommendation: add a diagram/table that distinguishes pipeline batching, transport retry, receiver backpressure, tap backpressure, and exporter lifecycle.

### Data Ownership and Mutation

OpenTelemetry Collector Go `consumer` APIs expose `Capabilities{MutatesData: true|false}` because Collector `pdata` values are mutable and fan-out must copy before mutating consumers.

`otlp4j` immutable records avoid this class of API complexity. This is a major strength and should be explicitly described as the reason there is no mutability capability flag.

### Metrics-Specific SDK Concerns

Go metric exporters expose temporality and aggregation selectors because they integrate with the Go Metrics SDK reader pipeline.

`otlp4j` transports already materialized OTLP metric data, so it does not need SDK aggregation selectors at this layer. If `otlp4j` later becomes a Java OpenTelemetry SDK exporter, that would be a separate API surface and should not be mixed into the current gateway pipeline API.

### Partial Success and Retry Semantics

OTLP responses support partial success fields such as rejected spans, rejected data points, rejected log records, rejected profiles, and error messages. `otlp4j` models this with `ConsumeResult.Partial<T>`, maps inbound partial success to `ConsumeResult`, and maps server-side partial results to OTLP partial-success responses. This is a strong alignment with OTLP.

The key difference is whole-batch failure. Go APIs generally return `error`; Collector receivers only acknowledge upstream after downstream `Consume*` returns successfully. `otlp4j` exposes `ConsumeResult.Rejected<T>`, and the bundled gRPC server maps that to gRPC errors rather than partial-success responses. This is coherent.

## Prioritized Recommendations

| Priority | Recommendation | Rationale |
| --- | --- | --- |
| P1 | Revisit `Metric.data()` nullability before API stabilization. | Null is inconsistent with the otherwise typed sealed model. |
| P2 | Add a `TelemetryTap` subscriber example. | `Flow.Publisher` is standard but verbose; users need one complete pattern. |
| P2 | Reduce classpath-visible internal public classes. | JPMS users are protected, classpath users are not. |

## Bottom Line

The API direction is strong: Java records plus a typed pipeline give a clearer application surface than generated OTLP protobufs, and the module boundary is well designed. The biggest improvements before wider use are not large rewrites. They are precise API/documentation fixes around the remaining standard-alignment gaps.
