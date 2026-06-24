# Public API

Application code normally depends on `otlp4j-api`, which transitively exposes `otlp4j-model`. Add `otlp4j-transport-grpc` and/or `otlp4j-transport-http` for the bundled OTLP/gRPC and OTLP/HTTP transports. Packages in the generated proto and codec modules are implementation details.

otlp4j is an OTLP gateway/pipeline library: receive, process, observe, route, and forward OTLP batches. It is not the OpenTelemetry Java instrumentation SDK. For tracer/meter/logger APIs, span lifecycle and `SpanContext`, context propagation, resource detectors, and metric instruments, use the official OpenTelemetry Java API/SDK. otlp4j complements that SDK when emitted telemetry should flow through an embedded JVM gateway.

Everything lives under the `dev.nthings.otlp4j` root. The types you import most often:

| Type(s) | Package |
| --- | --- |
| `OtlpGrpcExporter`, `OtlpGrpcReceiver` | `dev.nthings.otlp4j.transport.grpc` |
| `OtlpHttpExporter`, `OtlpHttpReceiver` | `dev.nthings.otlp4j.transport.http` |
| `Sink` (+ `TraceSink`, `MetricSink`, `LogSink`, `ProfileSink` aliases), `Source`, `Subscription`, `Telemetry`, `Drainable`, `Flushable` | `dev.nthings.otlp4j.core` |
| `Pipeline`, `Transform`, `FanOut` | `dev.nthings.otlp4j.pipeline` |
| `Receiver`, `TelemetryTap`, `TapOptions`, `BackpressureStrategy` | `dev.nthings.otlp4j.receiver` |
| `Exporter` | `dev.nthings.otlp4j.exporter` |
| `Transforms`, `BatchingProcessor`, `DropPolicy` | `dev.nthings.otlp4j.processor` |
| `Connector`, `Connectors`, `FailurePolicy` | `dev.nthings.otlp4j.connector` |
| Configuration: `ClientConfig`, `ServerConfig`, `Tls`, `Compression`, `RetryPolicy` | `dev.nthings.otlp4j.config` |
| Transport SPI: `OtlpClient`, `OtlpServer`, `Dispatchers` | `dev.nthings.otlp4j.spi` |
| Domain records: `TraceData`, `MetricsData`, `LogsData`, `ProfilesData`, `Resource`, `Attributes`, `AttributeValue`, `Span`, `Metric`, `LogRecord`, `Exemplar`, `ConsumeResult`, … | `dev.nthings.otlp4j.model` |

## If you know OpenTelemetry Go

`otlp4j` sits closer to a small Collector-style gateway than a single SDK exporter, so concepts map across rather than one-to-one:

| OpenTelemetry Go | otlp4j |
| --- | --- |
| `otlptracegrpc` / `otlpmetricgrpc` / `otlploggrpc` (one package per signal+transport) | One `OtlpGrpcExporter`; pick a signal facet — `exporter.traces()`, `.metrics()`, `.logs()`, `.profiles()`. |
| `go.opentelemetry.io/proto/otlp/...` generated protobuf | Proto-free immutable records in `dev.nthings.otlp4j.model` (`TraceData`, `MetricsData`, `LogsData`, `ProfilesData`, …). |
| Collector `consumer` (`ConsumeTraces(ctx, td) error`) | `TraceSink` (a `Sink<TraceData>`) returning `CompletionStage<ConsumeResult<TraceData>>`. |
| Collector `component` lifecycle (`Start`/`Shutdown(ctx)`) | `OtlpGrpcReceiver.start()`, `Subscription.shutdown(Duration)`, `Drainable`/`Flushable`. |
| Collector OTLP receiver | `OtlpGrpcReceiver` plus its per-signal `Source`s. |
| `exporterhelper` queue + retry + timeout | `BatchingProcessor` (queue), `RetryPolicy` (transport retry), per-request `timeout(...)`. |
| Functional options (`WithEndpoint`, `WithTimeout`, `WithInsecure`) | Builder methods (`endpoint(...)`, `timeout(...)`); the endpoint scheme decides plaintext vs TLS. |
| `consumer.Capabilities{MutatesData}` | Not needed — model records are immutable, so fan-out shares them without copying. |

Two deliberate differences: delivery is asynchronous (`CompletionStage<ConsumeResult<T>>`) with no per-call `context.Context`, and OTLP is carried over gRPC or HTTP with binary protobuf only (no `http/json`). Pick the transport by class — `OtlpGrpcExporter`/`OtlpGrpcReceiver` (port 4317) or `OtlpHttpExporter`/`OtlpHttpReceiver` (port 4318); the builders and pipeline wiring are identical. See [Sinks and results](#sinks-and-results) for the partial-success/retry mapping.

## Domain model

Each signal preserves OTLP's resource and instrumentation-scope grouping while providing a flattened accessor plus allocation-free traversal and count helpers:

| Batch | Grouping | Flattened accessor | `forEach` helper | Count helper |
| --- | --- | --- | --- | --- |
| `TraceData` | `ResourceSpans` → `ScopeSpans` → `Span` | `spans()` | `forEachSpan(Consumer)` | `spanCount()` |
| `MetricsData` | `ResourceMetrics` → `ScopeMetrics` → `Metric` | `metrics()` | `forEachMetric(Consumer)` | — |
| `LogsData` | `ResourceLogs` → `ScopeLogs` → `LogRecord` | `logRecords()` | `forEachLogRecord(Consumer)` | `logRecordCount()` |
| `ProfilesData` | `ResourceProfiles` → `ScopeProfiles` → `Profile` | `profiles()` | `forEachProfile(Consumer)` | `profileCount()` |

Each flattened accessor (`spans()`, `metrics()`, `logRecords()`, `profiles()`) walks the resource/scope grouping and allocates a fresh list on every call, so bind it to a local rather than re-calling it in a loop or on a hot path. On hot paths prefer the `forEach…` helper to visit items in the same order without the intermediate list, or the count helper to size a batch without flattening it — these are what batching and the count connectors use. (`MetricsData` has no naive count helper: the OTLP metric item count is nested data points, which the batching processor counts directly via `forEachMetric`.)

Records copy incoming lists and are safe to share between fan-out peers. `Attributes` and the sealed `AttributeValue` hierarchy represent OTLP values. Builders are available for `Attributes`, `Span`, `Metric`, and `LogRecord`, plus the metric data points (`NumberPoint`, `HistogramPoint`, `ExponentialHistogramPoint`) and `Exemplar`; `NumberPoint`, `Exemplar`, and `SummaryPoint` also offer `of(...)` factories for the common case. These builders are batch/model-construction helpers for OTLP payloads in the pipeline, not application instrumentation APIs. The remaining records use canonical constructors. To avoid hand-nesting the resource/scope wrappers, each signal type has an `of(resource, scope, items)` factory, and `Resource.of(...)` / `InstrumentationScope.of(...)` cover the common cases:

```java
var batch = TraceData.of(Resource.of(attributes), InstrumentationScope.of("my.lib", "1.0"), spans);
```

`Attributes.toBuilder()` plus `Builder.putAll(...)` make "copy and add a key" a one-liner instead of a manual map walk.

`Metric.Data` covers gauge, sum, histogram, exponential histogram, summary, and the empty `Metric.NoData` form. `Metric.data()` is never null: a metric whose wire form set no data (`DATA_NOT_SET`) carries `Metric.NoData` (build it with `Metric.noData()` / `Metric.NoData.INSTANCE`, or use `hasData()` / `dataOrThrow()` to skip the empty form). Switch over `data()` to handle every kind exhaustively. Number, histogram, and exponential-histogram points each carry a `List<Exemplar>` (`filteredAttributes`, `epochNanos`, `value`, `spanId`, `traceId`) mapped in both directions; the list is empty when no exemplars were recorded.

The `Metric.gauge/sum/histogram/exponentialHistogram/summary` factories build the sealed `Data` variants, and the point builders keep realistic construction terse and safe:

```java
// A gauge with one long point.
var gauge = Metric.builder()
        .name("queue.depth")
        .data(Metric.gauge(NumberPoint.of(attributes, epochNanos, NumberPoint.longValue(42))))
        .build();

// A monotonic cumulative counter.
var counter = Metric.builder()
        .name("http.server.requests")
        .data(Metric.sum(AggregationTemporality.CUMULATIVE, true,
                NumberPoint.of(attributes, epochNanos, NumberPoint.doubleValue(3.5))))
        .build();

// A histogram. Construction enforces the bucket invariant: bucketCounts has one
// more element than explicitBounds (or both are empty for a count/sum-only point).
var histogram = Metric.builder()
        .name("http.server.duration")
        .data(Metric.histogram(AggregationTemporality.DELTA,
                HistogramPoint.builder()
                        .count(10).sum(123.4).min(0.5).max(99.9)
                        .bucketCounts(List.of(2L, 3L, 5L))
                        .explicitBounds(List.of(1.0, 10.0))
                        .build()))
        .build();

// An exemplar linking a point back to a sampled span.
var exemplar = Exemplar.builder()
        .longValue(7).traceId(traceId).spanId(spanId).epochNanos(epochNanos)
        .build();
```

`ProfilesData` is marked `@Experimental` and forwards profiles losslessly via opaque passthrough. Its top-level `Profile` metadata is best-effort inspection only; each `Profile` also carries `rawProfile` (the serialized proto `Profile`) and the batch carries `dictionary` (the serialized `ProfilesDictionary`), so the payload re-emits byte-for-byte. Both are opaque `byte[]` — treat them as opaque and do not mutate; their accessors return defensive clones.

## Sinks and results

`Sink<T>` is the asynchronous pipeline contract:

```java
CompletionStage<ConsumeResult<T>> consume(T batch);
```

Prefer the signal aliases `TraceSink`, `MetricSink`, `LogSink`, and `ProfileSink` in declarations and extension APIs.

```java
TraceSink report = traces -> {
    System.out.println("spans=" + traces.spans().size());
    return ConsumeResult.acceptedStage();
};
```

`ConsumeResult<T>` has three variants:

- `Accepted<T>`: every item was accepted.
- `Partial<T>`: a positive item count was rejected; the rest was accepted.
- `Rejected<T>`: the complete batch was rejected.

`Accepted` and `Partial` are normal OTLP responses: `Accepted` leaves `partial_success` unset, and `Partial` carries the rejected count and message. A whole-batch `Rejected` is **not** a partial success (encoding it as `rejected_*=0` would read to the client as "all accepted"), so the bundled gRPC transport maps it to a gRPC error instead of a response message. The presence of a cause selects the retry semantics:

- `Rejected` with **no cause** → gRPC `UNAVAILABLE` (retryable); use it for transient back-pressure such as a full queue. Build it with `ConsumeResult.retryableRejected(message)`.
- `Rejected` with **a cause** → gRPC `INTERNAL` (non-retryable); use it for a permanent fault such as a policy or validation failure that would reject the same batch every time. Build it with `ConsumeResult.permanentRejected(message, cause)`.

Use an exception or exceptionally completed stage for a transport-level failure.

## Receive

`OtlpGrpcReceiver` defaults to plaintext `0.0.0.0:4317`; call `.tls(Tls.custom(cert, key, …))` on the builder to serve TLS. Port `0` selects an ephemeral port, available through `port()` after `start()`. A wildcard `bindHost` (empty, `0.0.0.0`, or `::`) binds every interface; any other host binds that specific interface, so `127.0.0.1` yields a loopback-only receiver.

`OtlpHttpReceiver` is the OTLP/HTTP counterpart with the same builder, defaulting to `0.0.0.0:4318`. It serves the standard signal paths (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`), inflates gzip request bodies, and — lacking a per-connection concurrency knob — bounds concurrency through `serverExecutor(...)` (a virtual-thread-per-request executor by default).

The receiver builder exposes the receiver-hardening knobs the bundled server applies directly (or set them on a `ServerConfig` and pass it through `transport(...)`), all defaulting to gRPC's own behaviour:

| Builder knob | Default | Effect |
| --- | --- | --- |
| `maxInboundMessageSizeBytes` | 4 MiB | Caps a single decoded export request; guards against memory-exhausting oversized requests. |
| `maxConcurrentCallsPerConnection` | `0` (gRPC default, unlimited) | A positive value caps in-flight calls per connection. |
| `handshakeTimeout` | 20s | Bounds the transport/TLS handshake only — not a slow request body or an idle connection. |
| `serverExecutor` | `null` (gRPC's own executor) | Supply a bounded pool to cap admitted concurrent work. |

```java
var receiver = OtlpGrpcReceiver.builder()
        .endpoint("0.0.0.0", 4317)
        .tls(Tls.custom(certFile, keyFile, trustFile))
        .maxInboundMessageSizeBytes(8 * 1024 * 1024)
        .maxConcurrentCallsPerConnection(256)
        .serverExecutor(Executors.newFixedThreadPool(32))
        .onTraces(report)
        .build()
        .start();
```

Compression is asymmetric: the server transparently decodes gzip request bodies via gRPC's default decoder and exposes no server compression knob (response compression is intentionally not configured).

**Production receiver checklist.** The plaintext `0.0.0.0:4317` default is convenient locally but unsafe to expose. Before running a receiver in production:

- **Bind host** — bind a specific interface (`127.0.0.1` for loopback) unless you mean to listen on every interface.
- **TLS** — serve TLS with `.tls(Tls.custom(cert, key, trust))`; require client certs (mTLS) on untrusted networks.
- **Inbound size cap** — set `maxInboundMessageSizeBytes` to bound a single decoded request.
- **Concurrency cap** — set `maxConcurrentCallsPerConnection` and/or a bounded `serverExecutor` to cap admitted work.
- **Handshake timeout** — keep `handshakeTimeout` tight to shed stalled TLS handshakes.
- **Auth** — credentials travel as gRPC metadata/headers; terminate auth in front of, or inside, your consumer. otlp4j does not authenticate requests itself.

```java
var receiver = OtlpGrpcReceiver.builder()
        .ephemeralPort()
        .onLogs(logs -> {
            System.out.println("records=" + logs.logRecords().size());
            return ConsumeResult.acceptedStage();
        })
        .build()
        .start();
```

Builder callbacks occupy the signal's single attachment slot. For a composed graph, leave the callback unset and attach through its source:

```java
Subscription subscription = Pipeline.from(receiver.traces())
        .filter(traces -> !traces.spans().isEmpty())
        .to(report);
```

Calling `consume` again while a source is attached throws `IllegalStateException`. Closing its subscription releases the slot. An unattached signal is acknowledged as accepted.

## Transform and route

Pipeline stages keep the signal type unchanged:

```java
var subscription = Pipeline.from(receiver.traces())
        .transform(Transforms.keepSpansWhere(
                span -> span.kind() == Span.Kind.SERVER))
        .transform(Transforms.withTracesResourceAttribute(
                "service.namespace", AttributeValue.of("store")))
        .filter(traces -> !traces.spans().isEmpty())
        .branch()
            .fanOut(exporter.traces())
            .fanOut(spanCounter)
        .join();
```

A branch can fan out to several **owned** resources. Register each on the linear stage with `owns(...)` (it is chainable) *before* `branch()`, so the subscription drains them all on shutdown under one shared deadline. Ownership is declared on the stage, not per peer:

```java
var primary = OtlpGrpcExporter.to("collector-a", 4317);
var secondary = OtlpGrpcExporter.to("collector-b", 4317);

var subscription = Pipeline.from(receiver.traces())
        .filter(traces -> !traces.spans().isEmpty())
        .owns(primary)
        .owns(secondary)
        .branch()
            .fanOut(primary.traces())
            .fanOut(secondary.traces())
        .join();
// subscription.shutdown(timeout) drains primary and secondary within one budget.
```

Available built-in transforms are span and log-record filters plus per-signal resource-attribute setters. Implement `Transform<T>` for other synchronous one-to-one rewrites.

`FanOut.of(...)` is the direct alternative to the branch builder. Peers run concurrently; the result is rejected if any peer rejects, otherwise partial results use the largest rejection count.

`Pipeline.peek(observer)` is a fire-and-forget side effect inside the main graph; it takes a plain `java.util.function.Consumer`, cannot alter or reject the batch, and swallows observer exceptions. For demand-aware streaming with bounded buffers, use the receiver's `TelemetryTap` instead.

The routing concepts, contrasted:

| Concept | Cardinality | Changes signal? | Owns downstream lifecycle? |
| --- | --- | --- | --- |
| `Transform` (via `Transforms`) | 1 → 1 | no | no |
| `Connector` (via `Connectors`) | 1 → 1 | yes | yes (its downstream consumer) |
| `BatchingProcessor` | N → 1 (buffered) | no | yes (when attached as terminal) |
| `Pipeline.peek` | observe | no | no |
| `TelemetryTap` (on the receiver) | observe (demand-aware) | no | n/a |

## Batch

Create a signal-specific batcher and attach it as the terminal consumer:

```java
var batcher = BatchingProcessor.forTraces()
        .downstream(exporter.traces())
        .maxBatchSize(512)
        .maxBatchAge(Duration.ofSeconds(5))
        .queueCapacity(2048)
        .dropPolicy(DropPolicy.DROP_NEWEST)
        .build();

var subscription = Pipeline.from(receiver.traces()).to(batcher);
```

The four factories are `forTraces`, `forMetrics`, `forLogs`, and `forProfiles`. `queued()` reports buffered batches, not telemetry item count. `droppedCount()` counts dropped batches.

Overflow behavior:

| Policy | Result |
| --- | --- |
| `DROP_OLDEST` | Evict the oldest queued batch and accept the new batch |
| `DROP_NEWEST` | Drop the new batch and return `Partial(1, ...)` |
| `BLOCK` | Block until queue space is available |
| `ERROR` | Drop the new batch and return `Rejected` |

`forceFlush` drains the current queue. `shutdown` stops the timer and drains once; the processor then rejects new batches. A pipeline subscription closes a directly attached batcher.

> [!WARNING]
> Profiles batching is constrained by the OTLP profile dictionary. A `ProfilesData` batch carries an opaque, batch-level `ProfilesDictionary` that each profile references by index, so merging is only lossless when the batches agree on that dictionary (a shared, or single non-empty, dictionary). `forProfiles()` merges only same-dictionary batches; a flush that drains profiles carrying **distinct non-empty dictionaries** fails — the merge throws and the whole drained batch surfaces as a `BatchDeliveryException` from `forceFlush`/`shutdown`, since re-indexing every reference is out of scope. Forward profiles 1:1 (do not batch them) unless every producer shares one dictionary.

## Export

`OtlpGrpcExporter` defaults to plaintext `localhost:4317` with a ten-second deadline per request. It owns one client channel and exposes a sink facet for each signal. TLS, authentication headers, gzip compression, and retries are available directly on the builder (`tls`, `header`/`headers`, `compression`, `retry`); pass a fully built `ClientConfig` through `transport(...)` only when you want to replace the whole config at once.

`OtlpHttpExporter` is the OTLP/HTTP counterpart with the identical builder, defaulting to `localhost:4318`. It POSTs each signal's binary protobuf to its standard path (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`) as `application/x-protobuf`; the scheme follows `tls` (`http`/`https`), `compression(GZIP)` sets `Content-Encoding: gzip`, and `retry` drives exponential-backoff retries over retryable statuses (408/429/502/503/504). An endpoint **path prefix** is applied: a collector behind `https://host/otlp` is reached at `/otlp/v1/traces`, set via the `OTEL_EXPORTER_OTLP_ENDPOINT` URL or the HTTP builder's `path(...)`. (gRPC ignores the path and uses the authority only.)

By default the exporter reads no environment — construction is fully explicit and deterministic. Opt in to the standard general OTLP variables with `fromEnvironment()` on the exporter or `ClientConfig` builder, or the static `OtlpGrpcExporter.fromEnvironment()` / `OtlpHttpExporter.fromEnvironment()` shorthand. It reads each variable only when present and only on that call; precedence is "call it first, explicit setters afterwards win"; malformed values throw. Only general (non-signal-specific) variables are read:

| Setting | Builder method | Environment variable | otlp4j default | Notes |
| --- | --- | --- | --- | --- |
| Endpoint host/port/scheme | `.endpoint(host, port)` / `.host` / `.port` / `.tls` | `OTEL_EXPORTER_OTLP_ENDPOINT` | gRPC `localhost:4317`, HTTP `localhost:4318`, plaintext | A URL; `http` is plaintext, `https` selects TLS. gRPC uses the authority as-is; HTTP appends the standard `/v1/<signal>` paths to any URL path prefix. A URL without a port keeps the exporter's protocol default (4317 gRPC, 4318 HTTP). |
| Endpoint path prefix (HTTP) | `.path(String)` (HTTP builder) | path component of `OTEL_EXPORTER_OTLP_ENDPOINT` | none | Prepended to `/v1/<signal>` — `https://host/otlp` → `/otlp/v1/traces`. Normalized (a bare `/` means none); gRPC ignores it. |
| Request timeout | `.timeout(Duration)` | `OTEL_EXPORTER_OTLP_TIMEOUT` | `10s` | Integer milliseconds; must be > 0. |
| Headers | `.header(k, v)` / `.headers(map)` / `.addHeaders(map)` | `OTEL_EXPORTER_OTLP_HEADERS` | none | `k=v,k2=v2`; values are percent-decoded (`+` stays literal). `.headers(map)` replaces all existing headers, while `.addHeaders(map)` and the env variable merge onto headers already set — env wins per key, unrelated keys are kept. |
| Compression | `.compression(Compression)` | `OTEL_EXPORTER_OTLP_COMPRESSION` | `NONE` | `gzip` or `none`. |
| Insecure (plaintext) | `.tls(Tls.disabled())` | `OTEL_EXPORTER_OTLP_INSECURE` | TLS off | `true`/`false`. Consulted only when no endpoint URL sets the scheme (a present endpoint's `http`/`https` wins); `true` forces plaintext, overriding the certificate variables. |
| Server CA / trust | `.tls(Tls.trust(path))` | `OTEL_EXPORTER_OTLP_CERTIFICATE` | system trust when TLS is on | Used when the endpoint is `https`, or — with no endpoint set — as an independent input that turns TLS on. Ignored on an `http` endpoint. |
| Client cert (mTLS) | `.tls(Tls.custom(cert, key, trust))` | `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` | none | Requires the client key; honoured with an `https` endpoint or, with no endpoint, as an independent input; ignored on an `http` endpoint. |
| Client key (mTLS) | `.tls(Tls.custom(cert, key, trust))` | `OTEL_EXPORTER_OTLP_CLIENT_KEY` | none | Requires the client certificate. |

Deliberately not read: `OTEL_EXPORTER_OTLP_PROTOCOL` (the protocol is chosen by which exporter class you instantiate, `OtlpGrpcExporter` vs `OtlpHttpExporter`, not by env). Signal-specific overrides such as `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` are out of scope: one exporter drives all four signals over a single connection, so a per-signal endpoint would need its own exporter — instantiate one `OtlpGrpcExporter`/`OtlpHttpExporter` per destination and route each signal to it through the pipeline. The same `fromEnvironment()` applies to `OtlpHttpExporter`, except a portless endpoint URL keeps the HTTP default port 4318.

```java
// Load env, then let an explicit endpoint win over OTEL_EXPORTER_OTLP_ENDPOINT.
var exporter = OtlpGrpcExporter.builder()
        .fromEnvironment()
        .endpoint("collector.example.com", 4317)
        .build();
```

```java
try (var exporter = OtlpGrpcExporter.builder()
        .endpoint("collector.example.com", 4317)
        .timeout(Duration.ofSeconds(5))
        .build()) {
    ConsumeResult<TraceData> result = exporter.traces()
            .consume(traces)
            .toCompletableFuture()
            .join();
}
```

A hardened exporter — system-trust TLS (or `Tls.custom(cert, key, trust)` for mTLS), a bearer header, gzip, and exponential retry:

```java
var exporter = OtlpGrpcExporter.builder()
        .endpoint("collector.example.com", 4317)
        .tls(Tls.systemTrust())
        .header("authorization", "Bearer " + token)
        .compression(Compression.GZIP)
        .retry(RetryPolicy.exponential(5, Duration.ofSeconds(1), Duration.ofSeconds(30)))
        .timeout(Duration.ofSeconds(10))
        .build();
```

The exporter itself owns the channel. Its `traces()`, `metrics()`, `logs()`, and `profiles()` facets are method references, so the pipeline cannot auto-discover the exporter behind them. Hand its lifecycle to the subscription with the two-arg terminal `to(exporter.traces(), exporter)` (equivalently, `Stage.owns(exporter)` before the terminal): it implements both `Drainable` and `Flushable`, so the subscription drains it on shutdown and reaches it on `forceFlush`. As a `Drainable` it receives the pipeline's *remaining* shared deadline, and its shutdown is cancellation-aware — a timeout interrupts the transport teardown rather than leaving it blocking past the deadline. `forceFlush` itself is a no-op today (the client holds no buffer) but is reachable once the exporter is owned. Without `owns`, close the exporter yourself.

Implement `Exporter<T>` for a custom typed terminal with flush and shutdown hooks. Implement the lower-level client or server SPI when replacing the wire transport.

## Connect signals

`Connector<I,O>` consumes one signal and emits another to its configured downstream consumer:

```java
var spanCounter = Connectors.spanCount(exporter.metrics());
var logCounter = Connectors.logRecordCount(exporter.metrics());

// Or fail the input batch when the derived metric is not delivered:
var strictSpanCounter = Connectors.spanCount(exporter.metrics(), FailurePolicy.FAIL);
```

The built-ins emit `otlp4j.connector.span.count` and `otlp4j.connector.log.record.count`, each as a monotonic delta sum whose window runs from the previous flush (so the series carries a real per-series start time). A configurable `FailurePolicy` decides how a downstream metric failure maps back onto the input result; the no-policy `spanCount`/`logRecordCount` overloads default to `BEST_EFFORT`, and `spanCount(downstream, policy)` / `logRecordCount(downstream, policy)` set it explicitly:

| Policy | Downstream `Partial`/`Rejected` | Input result |
| --- | --- | --- |
| `BEST_EFFORT` (default) | logged | `Accepted` — derived telemetry never fails the originating request |
| `FAIL` | logged | `Rejected`, so the caller learns the derived metric was not delivered |

An exceptionally completed downstream stage still propagates either way (a metric rejection cannot be relabeled as a trace or log rejection).

## Observe live traffic

Every receiver has independent JDK `Flow.Publisher` streams:

```java
receiver.tap().setOptions(
        new TapOptions(BackpressureStrategy.DROP_OLDEST, 512));

Flow.Publisher<TraceData> traces = receiver.tap().traces();
Flow.Publisher<Telemetry> allSignals = receiver.tap().all();
```

`all()` emits `Telemetry.Traces`, `.Metrics`, `.Logs`, or `.Profiles`. Defaults are a 256-batch buffer per subscription and `DROP_OLDEST`. Options apply when a subscriber attaches. `droppedCount()` aggregates tap drops for the receiver.

A tap stream is a standard `Flow.Publisher`; drive it with any `Flow.Subscriber` that manages its own demand and can cancel:

```java
receiver.tap().traces().subscribe(new Flow.Subscriber<TraceData>() {
    private Flow.Subscription subscription;

    @Override public void onSubscribe(Flow.Subscription s) {
        this.subscription = s;
        s.request(1);                 // demand-aware: ask for one batch at a time
    }

    @Override public void onNext(TraceData batch) {
        System.out.println("tapped spans=" + batch.spans().size());
        subscription.request(1);      // request the next only after handling this one
    }

    @Override public void onError(Throwable t) {
        // stream failed — e.g. the ERROR backpressure strategy overflowed
    }

    @Override public void onComplete() {
        // the receiver closed the tap
    }
});
```

Demand is independent of the receive/export acknowledgement path: a slow subscriber only drains its own bounded buffer under `TapOptions`, never back-pressuring the pipeline. Stop early with `subscription.cancel()`. For tests, `dev.nthings.otlp4j.testing.FlowSubscribers` (test scope) provides ready-made recording subscribers.

## Supply another transport

> For transport/extension authors. Application users can skip this section — the bundled `otlp4j-transport-grpc` and `otlp4j-transport-http` modules supply the OTLP/gRPC and OTLP/HTTP transports, and the `spi` package only matters when you implement a new wire transport.

There is no provider-discovery or `ServiceLoader` indirection: an application selects a transport by instantiating its concrete entry point — `OtlpGrpcExporter`/`OtlpGrpcReceiver` or `OtlpHttpExporter`/`OtlpHttpReceiver`. To add your own, implement the two transport-side contracts in `dev.nthings.otlp4j.spi` and wrap them in an exporter/receiver:

- `OtlpClient` (export) exposes `exportTraces`/`exportMetrics`/`exportLogs`/`exportProfiles`, each returning `CompletionStage<ConsumeResult<T>>`; extend `AbstractOtlpExporter` to adapt it into the per-signal `Sink` facets and lifecycle.
- `OtlpServer` (receive) is constructed with a `ServerConfig` and a `Dispatchers` record — the per-signal functions the server invokes as it decodes requests — and an `AbstractOtlpReceiver` drives it.

Configuration arrives through `ClientConfig` and `ServerConfig`. The bundled clients honour host, port, timeout, TLS, headers, compression, and retry; the servers honour their port, `bindHost`, TLS, and the receiver-hardening limits (inbound size cap, per-connection concurrency, handshake timeout, executor).

## Shutdown order

Stop ingestion before closing downstream resources:

1. Shut down the pipeline subscription to detach the source and drain every owned resource — directly attached processors plus anything registered with `Stage.owns(...)` (e.g. an exporter), all within a single shared deadline.
2. Close any resources you did not hand to the subscription: exporter instances not registered via `owns`, and connector downstreams (which the subscription does not auto-discover).
3. Shut down the receiver.

Use `shutdown(Duration)` when completion matters. The convenience `close()` methods drain gracefully with a fixed ten-second default across the receiver, exporter, and subscription; call `Receiver.shutdownNow()` for an immediate stop.
