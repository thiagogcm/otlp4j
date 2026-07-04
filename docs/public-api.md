# Public API

Application code normally depends on `otlp4j-api`, which transitively exposes `otlp4j-model`. Add `otlp4j-transport-grpc` and/or `otlp4j-transport-http` for the bundled OTLP/gRPC and OTLP/HTTP transports; each transitively brings in `otlp4j-transport-common` (package `dev.nthings.otlp4j.transport.common`), the shared layer the transport entry points compose (see [Supply another transport](#supply-another-transport)). Packages in the generated proto and codec modules are implementation details.

otlp4j is an OTLP gateway/pipeline library: receive, process, observe, route, and forward OTLP batches. It is not the OpenTelemetry Java instrumentation SDK. For tracer/meter/logger APIs, span lifecycle and `SpanContext`, context propagation, resource detectors, and metric instruments, use the official OpenTelemetry Java API/SDK. otlp4j complements that SDK when emitted telemetry should flow through an embedded JVM gateway.

## Start Here

Pick the path that matches your first task, then read the reference sections it links:

| Task | Path |
| --- | --- |
| **Receive and print**: stand up a receiver and observe incoming batches | [§ Receive and print](#receive-and-print), [Receive](#receive) |
| **Receive, transform, export**: filter/enrich a stream and forward it | [§ Receive, transform, export](#receive-transform-export), [Transform and route](#transform-and-route), [Export](#export) |
| **Construct and export**: build batches in code and send them | [§ Construct and export](#construct-and-export), [Domain model](#domain-model), [Export](#export) |

When you wire several stages together, read [Lifecycle](#lifecycle) so every resource is closed exactly once, and [Thread-safety and nullness](#thread-safety-and-nullness) for the concurrency and `null` contracts.

### Receive and print

```java
var receiver = OtlpGrpcReceiver.builder()
        .setEndpoint("127.0.0.1", 4317)
        .onTraces(traces -> {
            System.out.println("spans=" + traces.spanCount());
            return ConsumeResult.acceptedStage();
        })
        .build()
        .start();
// ... later: receiver.close();
```

### Receive, transform, export

Transforms are copy-modify: built-ins like `Transforms.withTracesResourceAttribute` return a new batch (records are immutable), so chain them and terminate with the exporter facet.

```java
var exporter = OtlpGrpcExporter.to("collector.example.com", 4317);

var subscription = Pipeline.from(receiver.traces())
        .transform(Transforms.keepSpansWhere(span -> span.kind() == Span.Kind.SERVER))
        .transform(Transforms.withTracesResourceAttribute(
                "service.namespace", AttributeValue.of("store")))
        .filter(traces -> !traces.spans().isEmpty())
        .to(exporter.traces());   // the facet drains the exporter on shutdown
```

### Construct and export

Build a batch with the `of(...)` factories and copy-modify helpers, then hand it to an exporter facet:

```java
var attrs = Attributes.builder().put("service.name", "checkout").build();
var batch = TracesData.of(
        Resource.of(attrs.with("deployment.environment", "prod")),   // copy-and-add one key
        InstrumentationScope.of("my.lib", "1.0"),
        spans);

try (var exporter = OtlpGrpcExporter.to("collector.example.com", 4317)) {
    exporter.traces().consume(batch).toCompletableFuture().join();
}
```

Everything lives under the `dev.nthings.otlp4j` root. The types you import most often:

| Type(s) | Package |
| --- | --- |
| `OtlpGrpcExporter`, `OtlpGrpcReceiver` | `dev.nthings.otlp4j.transport.grpc` |
| `OtlpHttpExporter`, `OtlpHttpReceiver` | `dev.nthings.otlp4j.transport.http` |
| `Pipeline`, `Sink` (+ `TracesSink`, `MetricsSink`, `LogsSink`, `ProfilesSink` aliases), `Source`, `Transform`, `AsyncTransform`, `FanOut`, `OtlpExporter`, `PipelineHandle`, `Lifecycle`, `SharedLifecycle`, `Wiring` | `dev.nthings.otlp4j.pipeline` |
| `Receiver`, `TelemetryTap`, `TapOptions` | `dev.nthings.otlp4j.receiver` |
| `Transforms`, `Batcher`, `OverflowPolicy`, `Connector`, `Connectors`, `FailurePolicy` | `dev.nthings.otlp4j.pipeline.ops` |
| Configuration: `ClientConfig`, `ServerConfig`, `Tls`, `Compression` | `dev.nthings.otlp4j.config` |
| Retry configuration: `RetryConfig` | `io.github.resilience4j.retry` |
| Transport SPI: `OtlpClient`, `OtlpServer`, `Dispatchers` | `dev.nthings.otlp4j.spi` |
| Domain records: `TracesData`, `MetricsData`, `LogsData`, `ProfilesData`, `Resource`, `Attributes`, `AttributeValue`, `Span`, `Metric`, `LogRecord`, `Exemplar`, `ConsumeResult`, ... | `dev.nthings.otlp4j.model` |

## If you know OpenTelemetry Go

`otlp4j` sits closer to a small Collector-style gateway than a single SDK exporter, so concepts map across rather than one-to-one:

| OpenTelemetry Go | otlp4j |
| --- | --- |
| `otlptracegrpc` / `otlpmetricgrpc` / `otlploggrpc` (one package per signal+transport) | One `OtlpGrpcExporter`; pick a signal facet: `exporter.traces()`, `.metrics()`, `.logs()`, `.profiles()`. |
| `go.opentelemetry.io/proto/otlp/...` generated protobuf | Proto-free immutable records in `dev.nthings.otlp4j.model` (`TracesData`, `MetricsData`, `LogsData`, `ProfilesData`, ...). |
| Collector `consumer` (`ConsumeTraces(ctx, td) error`) | `TracesSink` (a `Sink<TracesData>`) returning `CompletionStage<ConsumeResult>`. |
| Collector `component` lifecycle (`Start`/`Shutdown(ctx)`) | `OtlpGrpcReceiver.start()`, `PipelineHandle.shutdown(Duration)`, `Lifecycle`. |
| Collector OTLP receiver | `OtlpGrpcReceiver` plus its per-signal `Source`s. |
| `exporterhelper` queue + retry + timeout | `Batcher` (queue), Resilience4j `RetryConfig` (transport retry), `setTimeout(...)`. |
| Functional options (`WithEndpoint`, `WithTimeout`, `WithInsecure`) | Builder methods (`setEndpoint(...)`, `setTimeout(...)`); the endpoint scheme decides plaintext vs TLS. |
| `consumer.Capabilities{MutatesData}` | Not needed: model records are immutable, so fan-out shares them without copying. |

Two deliberate differences: delivery is asynchronous (`CompletionStage<ConsumeResult>`) with no per-call `context.Context`, and OTLP is carried over gRPC or HTTP with binary protobuf only (no `http/json`). Pick the transport by class: `OtlpGrpcExporter`/`OtlpGrpcReceiver` (port 4317) or `OtlpHttpExporter`/`OtlpHttpReceiver` (port 4318); the builders and pipeline wiring are identical. See [Sinks and results](#sinks-and-results) for the partial-success/retry mapping.

## Domain model

Each signal preserves OTLP's resource and instrumentation-scope grouping while providing a flattened accessor plus allocation-free traversal and count helpers:

| Batch | Grouping | Flattened accessor | `forEach` helper | Count helper |
| --- | --- | --- | --- | --- |
| `TracesData` | `ResourceSpans` -> `ScopeSpans` -> `Span` | `spans()` | `forEachSpan(Consumer)` | `spanCount()` |
| `MetricsData` | `ResourceMetrics` -> `ScopeMetrics` -> `Metric` | `metrics()` | `forEachMetric(Consumer)` | `dataPointCount()` |
| `LogsData` | `ResourceLogs` -> `ScopeLogs` -> `LogRecord` | `logRecords()` | `forEachLogRecord(Consumer)` | `logRecordCount()` |
| `ProfilesData` | `ResourceProfiles` -> `ScopeProfiles` -> `Profile` | `profiles()` | `forEachProfile(Consumer)` | `profileCount()` |

Each flattened accessor walks the resource/scope grouping and allocates a fresh list on every call, so bind it to a local rather than re-calling it in a loop or on a hot path. On hot paths prefer the `forEach...` helper to visit items in the same order without the intermediate list, or the count helper to size a batch without flattening it; these are what batching and the count connectors use. (`MetricsData`'s count helper, `dataPointCount()`, counts nested data points, the meaningful OTLP item count for metrics, rather than the number of `Metric` objects.)

Records copy incoming lists and are safe to share between fan-out peers. `Attributes` and the sealed `AttributeValue` hierarchy represent OTLP values. Builders are available for `Attributes`, `Span`, `Metric`, and `LogRecord`, plus the metric data points (`NumberPoint`, `HistogramPoint`, `ExponentialHistogramPoint`), `SummaryPoint`, and `Exemplar`; `NumberPoint`, `Exemplar`, and `SummaryPoint` also offer `of(...)` factories for the common case. These builders are batch/model-construction helpers for OTLP payloads in the pipeline, not application instrumentation APIs. Builder method naming follows one rule: model and pipeline-component builders (these and `Batcher.Builder`) use no-prefix fluent setters (`name(...)`, `flushThreshold(...)`), matching the immutable records and the JDK's own builders; the configuration and transport builders (`ClientConfig`, `ServerConfig`, and the `OtlpGrpc...`/`OtlpHttp...` entry points) use `set*` to mirror OpenTelemetry Java. The remaining records use canonical constructors. To avoid hand-nesting the resource/scope wrappers, each signal type has an `of(resource, scope, items)` factory, and `Resource.of(...)` / `InstrumentationScope.of(...)` cover the common cases:

```java
var batch = TracesData.of(Resource.of(attributes), InstrumentationScope.of("my.lib", "1.0"), spans);
```

The builder-backed records (`Span`, `Metric`, `LogRecord`, the points, `Exemplar`, `Attributes`) also have `toBuilder()` for copy-modify, and `Attributes.toBuilder()` plus `Builder.putAll(...)` (or the `Attributes.with(key, value)` overloads) make "copy and add a key" a one-liner instead of a manual map walk.

`Metric.Data` covers gauge, sum, histogram, exponential histogram, summary, and the empty `Metric.NoData` form. `Metric.data()` is never null: a metric whose wire form set no data (`DATA_NOT_SET`) carries `Metric.NoData` (build it with `Metric.noData()`, or use `hasData()` / `dataOrThrow()` to skip the empty form). Switch over `data()` to handle every kind exhaustively. Number, histogram, and exponential-histogram points each carry a `List<Exemplar>` (`filteredAttributes`, `epochNanos`, `value`, `spanId`, `traceId`) mapped in both directions; the list is empty when no exemplars were recorded.

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

`ProfilesData` is marked `@Experimental` and forwards profiles losslessly via opaque passthrough. Its top-level `Profile` metadata is best-effort inspection only; each `Profile` also carries `rawProfile` (the serialized proto `Profile`) and the batch carries `dictionary` (the serialized `ProfilesDictionary`), so the payload re-emits byte-for-byte. Both are opaque `byte[]`: treat them as opaque and do not mutate, since their accessors return defensive clones.

## Sinks and results

`Sink<T>` is the asynchronous pipeline contract:

```java
CompletionStage<ConsumeResult> consume(T batch);
```

`Sink<T>` keeps the input type parameter (a `TracesSink` cannot be fed `MetricsData`), while the result is signal-agnostic. Prefer the signal aliases `TracesSink`, `MetricsSink`, `LogsSink`, and `ProfilesSink` in declarations and extension APIs; APIs that _accept_ a sink take `Sink<? super T>`, so a lambda, an alias, a raw `Sink<TracesData>`, a `FanOut`, or a `Batcher` all fit.

```java
TracesSink report = traces -> {
    System.out.println("spans=" + traces.spans().size());
    return ConsumeResult.acceptedStage();
};
```

When a handler only needs accept/reject rather than the full `ConsumeResult` vocabulary, `Sink.accepting(action)` builds a sink from a synchronous action (a thrown exception becomes a permanent rejection) and `Sink.fromStage(fn)` from a `CompletionStage`-returning function, so you do not return `ConsumeResult.acceptedStage()` by hand. Implement the sink directly to report item-level partial success.

`ConsumeResult` has three variants:

- `Accepted`: every item was accepted.
- `Partial`: a positive item count was rejected; the rest was accepted.
- `Rejected`: the complete batch was rejected.

`Accepted` and `Partial` are normal OTLP responses: `Accepted` leaves `partial_success` unset, and `Partial` carries the rejected count and message. A whole-batch `Rejected` is **not** a partial success (encoding it as `rejected_*=0` would read to the client as "all accepted"), so the transports map it to a delivery error instead of a response message. `Rejected` states its retry intent explicitly; a diagnostic `cause` is optional on either disposition, and a retryable rejection may carry a `retryAfter` backpressure hint:

- A **retryable** `Rejected` maps to gRPC `UNAVAILABLE` / HTTP `503`; use it for transient back-pressure such as a full queue or a briefly unreachable downstream. Build it with `ConsumeResult.retryable(message)` or `retryable(message, cause)`.
- A **throttled** `Rejected` is a retryable rejection carrying a `retryAfter` hint, how long the sender should wait before retrying. Build it with `ConsumeResult.throttled(retryAfter, message)` or `throttled(retryAfter, message, cause)`. The transports put the hint on the wire: gRPC `UNAVAILABLE` with a `grpc-retry-pushback-ms` trailer, HTTP `503` with a `Retry-After` header (the hint rounded up to whole seconds, floored at 1 so a sub-second hint never collapses to `Retry-After: 0`). `retryAfter` is the fourth component of `Rejected` and is valid only on a retryable rejection; without a hint the mapping stays plain `UNAVAILABLE`/`503`.
- A **permanent** `Rejected` maps to gRPC `INTERNAL` / HTTP `500`; use it for a permanent fault such as a policy or validation failure that would reject the same batch every time. Build it with `ConsumeResult.permanent(message)` or `permanent(message, cause)`.

`ConsumeResult.fanOutMerge` (the fan-out aggregation) keeps the longest `retryAfter` across retryable peer rejections (retrying sooner cannot satisfy the slowest peer) and drops the hint when the merged rejection is permanent, since retrying cannot help.

Use an exception or exceptionally completed stage for a transport-level failure.

## Receive

`OtlpGrpcReceiver` defaults to plaintext `localhost:4317`; call `.setTls(Tls.custom(cert, key, ...))` on the builder to serve TLS. Port `0` selects an ephemeral port, available through `port()` after `start()`. A wildcard `bindHost` (empty, `0.0.0.0`, or `::`) binds every interface; any other host binds that specific interface, so `127.0.0.1` yields a fixed loopback-only receiver.

`OtlpHttpReceiver` is the OTLP/HTTP counterpart with the same builder, defaulting to `localhost:4318`. It serves the standard signal paths (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`), inflates gzip request bodies, and, lacking a per-connection concurrency knob, bounds concurrency through `setServerExecutor(...)` (a virtual-thread-per-request executor by default).

The receiver builder exposes the receiver-hardening knobs the bundled server applies directly (or set them on a `ServerConfig` and pass it through `setConfig(...)`), all defaulting to gRPC's own behaviour (the global admission caps default to disabled):

| Builder knob | Default | Effect |
| --- | --- | --- |
| `setMaxInboundMessageSizeBytes` | 4 MiB | Caps a single decoded export request; guards against memory-exhausting oversized requests. |
| `setMaxConcurrentCallsPerConnection` | `0` (gRPC default, unlimited) | A positive value caps in-flight calls per connection. |
| `setMaxInFlightRequests` | `0` (unlimited) | Caps decoded export requests in flight across **every** connection (global admission control); excess requests are rejected retryably. |
| `setMaxInFlightBytes` | `0` (unlimited) | Caps decoded request bytes in flight across every connection; a request is always admitted when nothing else is in flight, so a single oversized request cannot starve. |
| `setThrottleRetryAfter` | unset (no hint) | The retry-after hint attached to admission rejections, telling throttled senders how long to back off. |
| `setHandshakeTimeout` | 20s | Bounds the transport/TLS handshake only, not a slow request body or an idle connection. |
| `setServerExecutor` | `null` (gRPC's own executor) | Supply a bounded pool to cap admitted concurrent work. |

The two `setMaxInFlight*` caps are global admission control (Collector `memory_limiter`-style back-pressure), complementing the per-request size cap and the per-connection concurrency cap with a receiver-wide ceiling. A request that arrives at capacity is rejected retryably before dispatch, gRPC `UNAVAILABLE` with description `receiver at capacity` (plus a `grpc-retry-pushback-ms` trailer when `setThrottleRetryAfter` is set) or HTTP `429` (plus a `Retry-After` header when the hint is set), so well-behaved senders back off and retain their data. Compression is asymmetric: the server transparently decodes gzip request bodies and exposes no server compression knob.

```java
var receiver = OtlpGrpcReceiver.builder()
        .setEndpoint("0.0.0.0", 4317)
        .setTls(Tls.custom(certFile, keyFile, trustFile))
        .setMaxInboundMessageSizeBytes(8 * 1024 * 1024)
        .setMaxConcurrentCallsPerConnection(256)
        .setServerExecutor(Executors.newFixedThreadPool(32))
        .onTraces(report)
        .build()
        .start();
```

**Production receiver checklist.** The default loopback bind is convenient locally. Before exposing a receiver in production:

- **Bind host** keep the default loopback bind or choose a specific interface, unless you mean to listen on every interface with a wildcard host.
- **TLS** serve TLS with `.setTls(Tls.custom(cert, key, trust))`; require client certs (mTLS) on untrusted networks.
- **Concurrency and admission** cap admitted work with `setMaxConcurrentCallsPerConnection` and/or a bounded `setServerExecutor`, and set `setMaxInFlightRequests`/`setMaxInFlightBytes` (paired with `setThrottleRetryAfter`) so a burst across many connections is throttled at admission instead of exhausting memory.
- **Handshake timeout** keep `setHandshakeTimeout` tight to shed stalled TLS handshakes.
- **Auth** credentials travel as gRPC metadata/headers; otlp4j does not authenticate requests itself, so terminate auth in front of, or inside, your consumer.

Builder callbacks (`onTraces`/`onMetrics`/`onLogs`/`onProfiles`) occupy the signal's single attachment slot. For a composed graph, leave the callback unset and attach through its source:

```java
PipelineHandle subscription = Pipeline.from(receiver.traces())
        .filter(traces -> !traces.spans().isEmpty())
        .to(report);
```

Attaching a second consumer to an already-attached source throws `IllegalStateException`; closing its subscription releases the slot. An unattached signal is rejected retryably (`UNAVAILABLE` for gRPC, `503` for HTTP), so call `receiver.metrics().discard()` or the equivalent per-signal source when accepting and dropping that signal is intentional.

## Transform and route

Pipeline stages keep the signal type unchanged:

```java
var subscription = Pipeline.from(receiver.traces())
        .transform(Transforms.keepSpansWhere(
                span -> span.kind() == Span.Kind.SERVER))
        .transform(Transforms.withTracesResourceAttribute(
                "service.namespace", AttributeValue.of("store")))
        .filter(traces -> !traces.spans().isEmpty())
        .to(FanOut.of(exporter.traces(), spanCounter));
```

The built-in `Transforms` cover the common rewrites: `keepSpansWhere` / `keepLogRecordsWhere` (record filters that prune empty scopes and resources), `mapSpans` / `mapLogRecords` (rewrite every record 1:1, keeping empty groups), `map{Traces,Metrics,Logs,Profiles}Resource` (rewrite each `Resource`), and `with{Traces,Metrics,Logs,Profiles}ResourceAttribute` (add or overwrite one resource attribute). Implement `Transform<T>` for any other synchronous one-to-one rewrite. Transforms are strictly 1-to-1 and must return non-null: a null return is a contract violation the pipeline rejects permanently. To drop a batch, use `filter(...)`, typically after a pruning transform leaves the batch empty, as above.

`Stage.transformAsync(AsyncTransform<T>)` is the asynchronous counterpart: `AsyncTransform<T>` declares one method, `CompletionStage<T> applyAsync(T batch)`, for a 1-to-1 stage that needs I/O (an enrichment lookup, a remote sampling decision) off the synchronous dispatch path. The pipeline delivers downstream only once the returned stage completes, so the transport acknowledgement still covers the full chain. The same 1-to-1 contract applies: drop is not signalled by the async transform, so complete with a pruned (possibly empty) batch and compose `.transformAsync(sampler).filter(t -> t.spanCount() != 0)`; a thrown exception, a failed or null stage, or a null completion value each reject the batch permanently.

`FanOut.of(...)` is the one fan-out spelling: it returns a reusable `Sink<T>` that a stage terminates at with `.to(...)`. Peers run concurrently; the result is rejected if any peer rejects, otherwise a partial result uses the largest rejection count. The factories collapse degenerate shapes: a single peer is returned bare (so its lifecycle stays visible to the pipeline's auto-collection), and an **empty** peer set yields a no-op sink that **accepts and discards every batch** (matching `Source.discard()` semantics), so route an empty computed peer list deliberately.

To observe batches in-path, add an observing `Sink` as a fan-out peer: it runs concurrently with the terminal and shares the acknowledgement, so an observer that rejects also rejects the batch back to the sender. An identity `Transform` observes inline instead, but an observer that throws becomes a permanent rejection unless it catches its own exceptions. For demand-aware streaming with bounded buffers that stays outside the acknowledgement path, use the receiver's `TelemetryTap`.

When destinations have independent speeds or failure modes, give each its own queue: front each exporter with its own `Batcher` so a slow or failing collector drains its own buffer without stalling the others.

```java
var primaryQueue = Batcher.forTraces().downstream(primary.traces()).build();
var secondaryQueue = Batcher.forTraces().downstream(secondary.traces()).build();

var subscription = Pipeline.from(receiver.traces())
        .owns(primary)             // each exporter sits behind a batcher, so declare it (see Lifecycle)
        .owns(secondary)
        .to(FanOut.of(primaryQueue, secondaryQueue));
```

The routing concepts, contrasted:

| Concept | Cardinality | Changes signal? | Owns downstream lifecycle? |
| --- | --- | --- | --- |
| `Transform` (via `Transforms`) | 1 -> 1 | no | no |
| `AsyncTransform` (via `Stage.transformAsync`) | 1 -> 1 (asynchronous) | no | no; the stage itself is auto-collected when `AutoCloseable` |
| `Connector` (count sinks via `Connectors`) | 1 -> 1 | yes | yes, cascades to its downstream |
| `Batcher` | N -> 1 (buffered) | no | no; front its downstream with `owns(...)` |
| `TelemetryTap` (on the receiver) | observe (demand-aware) | no | n/a |

## Batch

Create a signal-specific batcher and attach it as the terminal consumer:

```java
var batcher = Batcher.forTraces()
        .downstream(exporter.traces())
        .flushThreshold(512)
        .queueCapacity(2048)
        .overflowPolicy(OverflowPolicy.DROP_NEWEST)
        .build();

var subscription = Pipeline.from(receiver.traces()).to(batcher);
```

The four factories are `forTraces`, `forMetrics`, `forLogs`, and `forProfilesUnsafe`. Both knobs count **batches**, not telemetry items: `flushThreshold` is the queue depth that triggers a drain, and `queueCapacity` is the hard cap (overflow follows the `OverflowPolicy`). `queued()` reports buffered batches, and `droppedCount()` counts dropped batches. Besides reaching the `flushThreshold`, a periodic one-second timer flushes whatever has queued, so a partly filled batch never waits indefinitely. Threading is cheap: one JVM-wide platform daemon thread (`otlp4j-batch-timer`) drives every batcher's flush timer, and each instance adds only a single virtual thread for its serial drain. `forceFlush` drains the current queue; `shutdown` stops the timer, drains once, then rejects new batches; a pipeline subscription closes a directly attached batcher.

> [!WARNING]
>
> Enqueueing acknowledges immediately: `consume` returns `Accepted` once the batch is queued, before it is delivered downstream. A timer- or threshold-triggered drain that fails downstream is logged at WARN and the drained batches are **lost with no upstream signal**; only `forceFlush` and `shutdown` surface a `BatchDeliveryException`. Opt in to `reflectDownstreamFailures(true)` when senders must learn about sustained downstream failure.

`reflectDownstreamFailures(true)` (default off) reflects sustained downstream failure back at ingress: after a failed drain the batcher rejects new batches retryably with a one-flush-interval retry-after hint **without enqueueing them** (the sender retains the batch instead of it joining a queue that is going nowhere) until the window expires (admitting a probe batch, so a healed downstream recovers without a stuck state) or a drain succeeds.

Overflow behavior (the `OverflowPolicy`):

| Policy | Result |
| --- | --- |
| `DROP_OLDEST` | Evict the oldest queued batch and accept the new batch |
| `DROP_NEWEST` | Drop the new batch and return `Partial` with its item count (`Accepted` if empty) |
| `BLOCK` | Block until queue space is available |
| `FAIL` | Drop the new batch and return a throttled retryable `Rejected` hinting one flush interval, the earliest retry that can find queue space |

> [!WARNING]
>
> Profiles batching is constrained by the OTLP profile dictionary. A `ProfilesData` batch carries an opaque, batch-level `ProfilesDictionary` that each profile references by index, so merging is only lossless when the batches agree on that dictionary (a shared, or single non-empty, dictionary). `forProfilesUnsafe()` merges only same-dictionary batches; a flush that drains profiles carrying **distinct non-empty dictionaries** fails (the merge throws and the whole drained batch surfaces as a `BatchDeliveryException` from `forceFlush`/`shutdown`), since re-indexing every reference is out of scope. Forward profiles 1:1 unless every producer shares one dictionary.

## Export

`OtlpGrpcExporter` defaults to plaintext `localhost:4317` with a ten-second export deadline. It owns one client channel and exposes a sink facet for each signal. Set the endpoint as a single `setEndpoint(url)` URL or as `setEndpoint(host, port)`; TLS, authentication headers, gzip compression, and retries are configured directly on the builder (`setTls`, `addHeader`/`setHeaders`, `setCompression`, `setRetryConfig`). Retries are **on by default** through `ClientConfig.defaultRetryConfig()`: five attempts, exponential randomized backoff from 1s to 5s with a 1.5x multiplier; pass `RetryConfig.custom().maxAttempts(1).build()` to opt out. `setHeaders(Supplier<Map<String,String>>)` supplies headers evaluated per export (for a rotating bearer token), overlaid on the static headers (the supplier wins per key). `setTls` accepts PEM file paths, in-memory PEM bytes (`Tls.custom(byte[]...)` / `Tls.trust(byte[])`), or a caller-built `SSLContext` (`Tls.sslContext(context, trustManager)`). Pass a fully built `ClientConfig` through `setConfig(...)` only to replace the whole config at once.

Headers are validated when the `ClientConfig` is built, so an invalid static header fails fast at configuration time instead of on the export path. The rules are the intersection both bundled transports accept: names must be non-empty, drawn from `[0-9a-zA-Z_.-]`, and not the gRPC-reserved `-bin` suffix; values are visible ASCII plus space and horizontal tab (no CR/LF header injection, no NUL, no non-ASCII). Validation failures name the offending key but never echo its value, which may be a credential. Supplier-provided headers are re-validated on every export; an invalid supplied header fails that export exceptionally and is not retried.

`OtlpHttpExporter` is the OTLP/HTTP counterpart with the identical builder, defaulting to `localhost:4318`. It POSTs each signal's binary protobuf to its standard path (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`) as `application/x-protobuf`; the scheme follows `setTls` (`http`/`https`), `setCompression(GZIP)` sets `Content-Encoding: gzip`, and `setRetryConfig` uses the same Resilience4j retry path as gRPC over retryable statuses (408/429/502/503/504) and I/O failures. HTTP `Retry-After` and gRPC `grpc-retry-pushback-ms` fold into the Resilience4j interval function, and the configured timeout bounds the export stage across attempts. An endpoint **path prefix** is applied: a collector behind `https://host/otlp` is reached at `/otlp/v1/traces`, set via the `setEndpoint(url)` / `OTEL_EXPORTER_OTLP_ENDPOINT` URL or the HTTP builder's `setPath(...)` (gRPC ignores the path and uses the authority only). `setConnectTimeout(Duration)` bounds connection setup, falling back to the request timeout when unset; it applies to OTLP/HTTP only (gRPC has no separate connect timeout and ignores it).

By default the exporter reads no environment: construction is fully explicit and deterministic. Opt in to the standard general OTLP variables with `fromEnvironment()` on the exporter or `ClientConfig` builder, or the static `OtlpGrpcExporter.fromEnvironment()` / `OtlpHttpExporter.fromEnvironment()` shorthand. It reads each variable only when present; environment values are **lowest precedence and order-independent**, applied at `build()` only where you did not set the field explicitly, so call order does not matter. Malformed values throw. Only general (non-signal-specific) variables are read:

| Setting | Builder method | Environment variable | otlp4j default | Notes |
| --- | --- | --- | --- | --- |
| Endpoint host/port/scheme | `.setEndpoint(url)` / `.setEndpoint(host, port)` / `.setTls` | `OTEL_EXPORTER_OTLP_ENDPOINT` | gRPC `localhost:4317`, HTTP `localhost:4318`, plaintext | A URL; `http` is plaintext, `https` selects TLS. gRPC uses the authority as-is; HTTP appends the standard `/v1/<signal>` paths to any URL path prefix. A URL without a port keeps the exporter's protocol default (4317 gRPC, 4318 HTTP). |
| Endpoint path prefix (HTTP) | `.setPath(String)` (HTTP builder) | path component of `OTEL_EXPORTER_OTLP_ENDPOINT` | none | Prepended to `/v1/<signal>`: `https://host/otlp` -> `/otlp/v1/traces`. Normalized (a bare `/` means none); gRPC ignores it. |
| Request timeout | `.setTimeout(Duration)` | `OTEL_EXPORTER_OTLP_TIMEOUT` | `10s` | Integer milliseconds; must be > 0. |
| Headers | `.addHeader(k, v)` / `.setHeaders(map)` | `OTEL_EXPORTER_OTLP_HEADERS` | none | `k=v,k2=v2`; values are percent-decoded (`+` stays literal). `.setHeaders(map)` replaces all static headers; explicit keys win, and the env variable fills only keys you did not set. `setHeaders(Supplier<Map>)` (no env form) supplies headers per export, overlaid on the static headers. |
| Compression | `.setCompression(Compression)` / `.setCompression(String)` | `OTEL_EXPORTER_OTLP_COMPRESSION` | `NONE` | `gzip` or `none`. |
| Insecure (plaintext) | `.setTls(Tls.disabled())` | `OTEL_EXPORTER_OTLP_INSECURE` | TLS off | `true`/`false`. Consulted only when no endpoint URL sets the scheme (a present endpoint's `http`/`https` wins); `true` forces plaintext, overriding the certificate variables. |
| Server CA / trust | `.setTls(Tls.trust(path))` | `OTEL_EXPORTER_OTLP_CERTIFICATE` | system trust when TLS is on | Used when the endpoint is `https`, or (with no endpoint set) as an independent input that turns TLS on. Ignored on an `http` endpoint. |
| Client cert (mTLS) | `.setTls(Tls.custom(cert, key, trust))` | `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` | none | Requires the client key; honoured with an `https` endpoint or, with no endpoint, as an independent input; ignored on an `http` endpoint. |
| Client key (mTLS) | `.setTls(Tls.custom(cert, key, trust))` | `OTEL_EXPORTER_OTLP_CLIENT_KEY` | none | Requires the client certificate. |

Deliberately not read: `OTEL_EXPORTER_OTLP_PROTOCOL` (the protocol is chosen by which exporter class you instantiate, not by env). Signal-specific overrides such as `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` are out of scope: one exporter drives all four signals over a single connection, so a per-signal endpoint would need its own exporter (instantiate one `OtlpGrpcExporter`/`OtlpHttpExporter` per destination and route each signal to it through the pipeline).

```java
// Env fills the gaps; the explicit endpoint wins over OTEL_EXPORTER_OTLP_ENDPOINT
// regardless of the order these are called.
var exporter = OtlpGrpcExporter.builder()
        .fromEnvironment()
        .setEndpoint("collector.example.com", 4317)
        .build();
```

A hardened exporter (an `https` URL endpoint selecting system-trust TLS, or `.setTls(Tls.custom(cert, key, trust))` for mTLS, a bearer header, gzip, and Resilience4j exponential retry with jitter):

```java
var exporter = OtlpGrpcExporter.builder()
        .setEndpoint("https://collector.example.com:4317")
        .addHeader("authorization", "Bearer " + token)
        .setCompression(Compression.GZIP)
        .setRetryConfig(RetryConfig.custom()
                .maxAttempts(5)
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
                        Duration.ofSeconds(1), 1.5, 0.5, Duration.ofSeconds(30)))
                .build())
        .setTimeout(Duration.ofSeconds(10))
        .build();
```

`OtlpGrpcExporter`/`OtlpHttpExporter` are builder entry points; `build()` (and the `to(...)`/`fromEnvironment()` shortcuts) returns an `OtlpExporter`, the multi-signal export contract (the counterpart to `Receiver` on the ingest side). The exporter owns the channel, and its `traces()`, `metrics()`, `logs()`, and `profiles()` facets are lifecycle-bearing views that deliver to the client over that one channel. `forceFlush` is a no-op today (the client holds no buffer). If you call a facet's `consume` outside a pipeline, close the exporter yourself; see [Lifecycle](#lifecycle) for how attaching a facet drains the exporter automatically.

Implement `Sink<T>` plus `Lifecycle` for a custom typed terminal with flush and shutdown hooks. Implement the lower-level client or server SPI when replacing the wire transport.

## Connect signals

A connector is a signal-changing bridge: it consumes batches of one signal and emits derived batches of another (or the same) signal to a single downstream sink, the general form of the Collector's connector, embodied by the public abstract base `pipeline.ops.Connector<I, O>`. The bundled count sinks are `Connector` instances: they consume one signal and emit a derived metric to a configured downstream `MetricsSink`. `Connectors.spanCount` returns a `TracesSink`; `Connectors.logRecordCount` returns a `LogsSink`. Wire them in like any other sink, as a terminal or a fan-out peer:

```java
var spanCounter = Connectors.spanCount(exporter.metrics());
var logCounter = Connectors.logRecordCount(exporter.metrics());

// Or fail the input batch when the derived metric is not delivered:
var strictSpanCounter = Connectors.spanCount(exporter.metrics(), FailurePolicy.FAIL);
```

A count sink cascades its lifecycle to the downstream `MetricsSink` it wraps, so attaching the connector drains that downstream exporter automatically, with no `Stage.owns(...)` (see [Lifecycle](#lifecycle)).

The built-ins emit `otlp4j.connector.span.count` and `otlp4j.connector.log.record.count`, each as a monotonic delta sum whose window runs from the previous flush (so the series carries a real start time). A configurable `FailurePolicy` decides how a downstream metric failure maps back onto the input result; the no-policy `spanCount`/`logRecordCount` overloads default to `BEST_EFFORT`, and the `spanCount(downstream, policy)` / `logRecordCount(downstream, policy)` overloads set it explicitly:

| Policy | Downstream `Partial`/`Rejected` (or a `derive` throw) | Input result |
| --- | --- | --- |
| `BEST_EFFORT` (default) | logged | `Accepted`: derived telemetry never fails the originating request |
| `FAIL` | logged | `Rejected`, so the caller learns the derived metric was not delivered |

A `derive` throw or an exceptionally completed downstream stage is treated like any other derived-delivery failure and follows the policy above (absorbed under `BEST_EFFORT`, surfaced as a `Rejected` under `FAIL`). Only an `Error` is always rethrown, never mapped to a result.

### Write your own connector

Extend `Connector<I, O>` and implement one synchronous method, `protected @Nullable O derive(I batch)`: return the derived batch, or `null` to emit nothing and accept the input. The base class finalizes `consume`, `retain`, `shutdown`, and `forceFlush`, keeping the `FailurePolicy` contract and the shared-downstream reference-count balance correct by construction (a `derive` throw follows the policy like any other derived-delivery failure). The constructors are `Connector(name, downstream)` (defaulting to `BEST_EFFORT`) and `Connector(name, downstream, policy)`; the name labels log and rejection messages. A connector holding local state overrides the `onShutdown`/`onForceFlush` hooks, which run before the downstream cascade so a buffering connector flushes into a still-open downstream. A connector needing asynchronous derivation or multi-batch emission implements `Sink<I>` directly instead (composing an `AsyncTransform` before the connector covers most async pre-steps).

```java
// Traces in, logs out: one log record per error span.
final class ErrorSpanLogs extends Connector<TracesData, LogsData> {

    private static final InstrumentationScope SCOPE = InstrumentationScope.of("error-span-logs", "1.0");

    ErrorSpanLogs(LogsSink downstream) {
        super("error-span-logs", downstream);
    }

    @Override
    protected @Nullable LogsData derive(TracesData batch) {
        var records = batch.spans().stream()
                .filter(span -> span.status().code() == Span.Status.Code.ERROR)
                .map(span -> LogRecord.builder().body("error span: " + span.name()).build())
                .toList();
        return records.isEmpty() ? null : LogsData.of(Resource.EMPTY, SCOPE, records);
    }
}

var subscription = Pipeline.from(receiver.traces())
        .to(FanOut.of(exporter.traces(), new ErrorSpanLogs(exporter.logs())));
```

## Observe live traffic

Every receiver has independent per-signal JDK `Flow.Publisher` streams:

```java
Flow.Publisher<TracesData> traces = receiver.tap().traces();   // TapOptions.defaults()
Flow.Publisher<TracesData> bounded =
        receiver.tap().traces(new TapOptions(TapOptions.Overflow.DROP_OLDEST, 512));
```

Each signal has a no-arg publisher (a 256-batch buffer per subscription with `DROP_OLDEST`) and a `TapOptions`-taking overload that binds the buffer size and overflow strategy to that publisher's next subscription, so options travel with the subscription rather than tap-wide mutable state. `TapOptions` takes its own nested `TapOptions.Overflow` enum (`DROP_OLDEST`, `DROP_NEWEST`, `FAIL`), not the batcher's `OverflowPolicy`: every tap strategy resolves immediately, and there is deliberately no `BLOCK` for taps, so a slow subscriber can never delay the OTLP acknowledgement. `droppedCount()` aggregates tap drops for the receiver.

A tap stream is a standard `Flow.Publisher`; drive it with any `Flow.Subscriber` that manages its own demand and can cancel:

```java
receiver.tap().traces().subscribe(new Flow.Subscriber<TracesData>() {
    private Flow.Subscription subscription;

    @Override public void onSubscribe(Flow.Subscription s) {
        this.subscription = s;
        s.request(1);                 // demand-aware: ask for one batch at a time
    }

    @Override public void onNext(TracesData batch) {
        System.out.println("tapped spans=" + batch.spans().size());
        subscription.request(1);      // request the next only after handling this one
    }

    @Override public void onError(Throwable t) {
        // stream failed, e.g. the FAIL overflow policy terminated it on buffer overflow
    }

    @Override public void onComplete() {
        // the receiver closed the tap
    }
});
```

Demand is independent of the receive/export acknowledgement path: a slow subscriber only drains its own bounded buffer under `TapOptions`, never back-pressuring the pipeline. Stop early with `subscription.cancel()`. For tests, `dev.nthings.otlp4j.testing.FlowSubscribers` (test scope) provides ready-made recording subscribers.

## Supply another transport

> For transport/extension authors. Application users can skip this section: the bundled `otlp4j-transport-grpc` and `otlp4j-transport-http` modules supply the OTLP/gRPC and OTLP/HTTP transports, and the `spi` package only matters when you implement a new wire transport.

There is no provider-discovery or `ServiceLoader` indirection: an application selects a transport by instantiating its concrete entry point. To add your own, implement the two transport-side contracts in `dev.nthings.otlp4j.spi` and wrap them in an exporter/receiver:

- `OtlpClient` (export) exposes `exportTraces`/`exportMetrics`/`exportLogs`/`exportProfiles`, each returning `CompletionStage<ConsumeResult>`.
- `OtlpServer` (receive) is constructed with a `ServerConfig` and a `Dispatchers` record, the per-signal functions the server invokes as it decodes requests.

You do not implement the user-facing surface yourself: module `otlp4j-transport-common` (package `dev.nthings.otlp4j.transport.common`) is the shared layer the bundled entry points compose, and yours composes it the same way. Wrap your `OtlpClient` in a `ClientExporter` to get the `OtlpExporter` contract (the per-signal `Sink` facets, reference-counted channel lifecycle, and the garbage-collection leak warning) for free; wrap your `OtlpServer` in a `ServerReceiver`, which builds the `Dispatchers`, hands them to your server factory, and exposes one `Source` per signal plus the `TelemetryTap`, dispatch wiring, and lifecycle. For a fluent builder, extend `AbstractExporterBuilder`/`AbstractReceiverBuilder`: each holds the `ClientConfig.Builder`/`ServerConfig.Builder` it delegates the shared setters to, while your subclass supplies the wire client or server (`newClient`/`newServer`) and adds transport-specific knobs through `config()`. When a custom exporter shares one connection behind several facets, implement `pipeline.SharedLifecycle`: a subscription calls `retain()` once when it collects the resource, and the handle is released only when the last retained owner shuts down.

For a custom _terminal_ rather than a new wire protocol (somewhere telemetry lands that isn't OTLP at all), implement `Sink<T>` plus `Lifecycle` (see [Export](#export)) instead of the transport SPI.

Configuration arrives through `ClientConfig` and `ServerConfig`. The bundled clients honour host, port, timeout, TLS, headers, compression, and retry; the servers honour their port, `bindHost`, TLS, and the receiver-hardening limits (inbound size cap, per-connection concurrency, global in-flight request/byte admission caps with their throttle retry-after hint, handshake timeout, executor). `AdmissionControl` in `otlp4j-transport-common` implements the global caps; a custom server acquires a permit per decoded request and rejects retryably at capacity like the bundled transports.

## Lifecycle

Shut the receiver down first, then the subscription. `receiver.shutdown(timeout)` stops the receiver accepting new requests and drains the in-flight ones through the still-live pipeline and exporter; closing the subscription afterward detaches the source instantly and drains everything it references within one shared deadline. Draining the receiver first keeps live senders reaching a working exporter for the whole window, the topological order the Collector uses, stopping receivers before the exporters they feed.

**What a subscription drains, and what you must declare.** A subscription drains every terminal and fan-out peer it can see that implements `AutoCloseable`:

- **Exporter facets** carry their exporter's lifecycle, so attaching one drains the whole exporter with nothing to register.
- A **directly attached `Batcher`** drains its own queue on shutdown.
- An **`AutoCloseable`/`Lifecycle` `AsyncTransform`** stage is auto-collected and drained before the terminal's resources.
- A **count `Connector`** cascades its shutdown to the downstream metric sink it wraps.

Use `Stage.owns(resource)` only for a resource the subscription **cannot** see: one hidden behind a bare lambda terminal, or an exporter fronted by a `Batcher` (which drains its own queue but not its downstream). `owns(...)` is chainable and must be declared on the stage _before_ `.to(...)`.

```java
// A bare-lambda terminal hides the exporter it delegates to, so declare it.
var sideExporter = OtlpGrpcExporter.to("side-collector", 4317);
var subscription = Pipeline.from(receiver.traces())
        .owns(sideExporter)
        .to(traces -> sideExporter.traces().consume(traces));
```

Collected resources are deduplicated by reference identity, so a resource reachable twice (say as a fan-out peer and again through an `owns(...)` declaration) is retained and shut down once. When two subscriptions attach facets of the _same_ exporter, its channel closes only once both have shut down (`SharedLifecycle` reference counting). A resource the subscription never drains is not silently lost: an exporter surfaces as a warning logged by its own GC cleaner when it is collected having never been shut down.

Use `shutdown(Duration)` when completion matters. The convenience `close()` methods drain gracefully within a fixed ten-second grace period (`Lifecycle.DEFAULT_GRACE_PERIOD`), applied uniformly by every `Lifecycle` (receiver, exporter, batcher, and subscription); call `Receiver.shutdownNow()` for an immediate stop.

### Wiring

`Wiring` codifies that ordering into one handle over the whole gateway: its receivers, its pipeline subscriptions, and any resources hidden from every pipeline:

```java
try (var wiring = Wiring.builder()
        .receiver(receiver)
        .pipeline(tracesSubscription)
        .pipeline(metricsSubscription)
        .owns(sideExporter)               // reachable only through a lambda terminal
        .build()) {
    // serve until shutdown
}
```

Shutdown runs receivers first (nothing new is ingested), then pipeline subscriptions (in-flight work and their collected resources drain), then directly owned resources, all within one shared deadline. It is best-effort (a failing step never blocks the rest, and the first error is rethrown once everything has been attempted), and a second `shutdown` is a no-op. Resources are deduplicated by reference identity across all three phases, so a resource registered twice is shut down once. `owns(...)` takes any `AutoCloseable`; a `SharedLifecycle` (such as a shared exporter) is retained once at `build()`, so handing an exporter to the wiring _and_ attaching its facets in member pipelines stays reference-count balanced.

## Thread-safety and nullness

**Thread-safety.** The runtime types are safe to share across threads; the _builders_ are not.

| Type | Contract |
| --- | --- |
| `OtlpGrpcReceiver` / `OtlpHttpReceiver` | Thread-safe after `start()`; dispatch incoming requests to sinks concurrently. The `...Receiver.Builder` is single-threaded: configure on one thread, then `build()`. |
| `OtlpGrpcExporter` / `OtlpHttpExporter` | Thread-safe: a single exporter owns one client/channel and may be called from many threads; each facet's `consume` is concurrency-safe. The builder is single-threaded. |
| `Batcher` | Thread-safe: `consume`, `forceFlush`, and `shutdown` may be called concurrently (queue + counters are concurrent). `shutdown` is idempotent and then rejects new batches. |
| `PipelineHandle` (from `Pipeline.from(...)`) | `shutdown`/`forceFlush` are safe to call concurrently and idempotently. Build the pipeline on one thread. |
| `TelemetryTap` | Thread-safe: each publisher multicasts independently and `TapOptions` bind to a subscription when it attaches. Each `Flow.Subscriber` manages its own demand. |
| Model records (`TracesData`, `Attributes`, ...) | Immutable and freely shareable; fan-out peers share them without copying. Their builders are single-threaded. |

`close()` blocks on the drain (it calls `shutdown(...).join()`), so do not call it from a pipeline or completion thread the drain itself needs; that thread can stall until the deadline. From inside async code, call `shutdown(Duration)` and compose the returned stage instead.

**Nullness.** Every public package is `@NullMarked`: parameters, return types, and fields are non-null unless explicitly annotated `@Nullable`. The intentionally nullable spots in the public surface are:

- **Receiver builder signal callbacks** (`onTraces`/`onMetrics`/`onLogs`/`onProfiles`) are optional; an unset signal slot is rejected retryably unless the source is explicitly attached or discarded.
- **`serverExecutor`** (receiver builder / `ServerConfig`): `null` keeps gRPC's own executor.
- **`Tls.custom(cert, key, trustFile)`**: a `null` `trustFile` falls back to system trust. Besides PEM file paths, `Tls` accepts in-memory PEM bytes (`Tls.custom(byte[]...)` / `Tls.trust(byte[])`) and a caller-built `SSLContext` (`Tls.sslContext(SSLContext, X509TrustManager)`). The `SSLContext` variant is used verbatim on OTLP/HTTP (a client certificate it carries is presented for mTLS); on OTLP/gRPC only the `X509TrustManager` is used for server verification, since gRPC has no `SSLContext` door, so use `Tls.custom(...)` (paths or bytes) for gRPC client certificates. Both servers reject `Tls.sslContext` and `Tls.systemTrust`, which carry no server certificate.
- **`ConsumeResult.Rejected.cause`**: an optional diagnostic throwable, orthogonal to retryability; `null` when none is available. **`ConsumeResult.Rejected.retryAfter`** is likewise `@Nullable`: `null` means no backpressure hint, and a non-null hint is valid only on a retryable rejection. See [Sinks and results](#sinks-and-results).
- **Point values**: `NumberPoint`/`Exemplar` value may be `null` for a value-unset point.
- **`Attributes.get(key)`** returns `null` on a lookup miss, as does the typed `getString(key)`.

`Batcher.Builder.downstream` must be set before `build()`; building without it throws rather than accepting `null`.
