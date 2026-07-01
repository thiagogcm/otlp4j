# Public API

Application code normally depends on `otlp4j-api`, which transitively exposes `otlp4j-model`. Add `otlp4j-transport-grpc` and/or `otlp4j-transport-http` for the bundled OTLP/gRPC and OTLP/HTTP transports. Packages in the generated proto and codec modules are implementation details.

otlp4j is an OTLP gateway/pipeline library: receive, process, observe, route, and forward OTLP batches. It is not the OpenTelemetry Java instrumentation SDK. For tracer/meter/logger APIs, span lifecycle and `SpanContext`, context propagation, resource detectors, and metric instruments, use the official OpenTelemetry Java API/SDK. otlp4j complements that SDK when emitted telemetry should flow through an embedded JVM gateway.

## Start Here

Pick the path that matches your first task, then read the reference sections it links:

| Task                                                                     | Path                                                                                                                       |
| ------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| **Receive and print** — stand up a receiver and observe incoming batches | [§ Receive and print](#receive-and-print) → [Receive](#receive)                                                            |
| **Receive, transform, export** — filter/enrich a stream and forward it   | [§ Receive, transform, export](#receive-transform-export) → [Transform and route](#transform-and-route), [Export](#export) |
| **Construct and export** — build batches in code and send them           | [§ Construct and export](#construct-and-export) → [Domain model](#domain-model), [Export](#export)                         |

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

Transforms are copy-modify: built-ins like `Transforms.withTracesResourceAttribute` return a new batch (records are immutable), so chain them and terminate with the exporter facet, which drains the exporter on shutdown.

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

| Type(s)                                                                                                                                                                            | Package                             |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `OtlpGrpcExporter`, `OtlpGrpcReceiver`                                                                                                                                             | `dev.nthings.otlp4j.transport.grpc` |
| `OtlpHttpExporter`, `OtlpHttpReceiver`                                                                                                                                             | `dev.nthings.otlp4j.transport.http` |
| `Pipeline`, `Sink` (+ `TraceSink`, `MetricSink`, `LogSink`, `ProfileSink` aliases), `Source`, `Transform`, `FanOut`, `PipelineHandle`, `Lifecycle`                                  | `dev.nthings.otlp4j.pipeline`       |
| `Receiver`, `TelemetryTap`, `TapOptions`, `Telemetry`                                                                                                                              | `dev.nthings.otlp4j.receiver`       |
| `OtlpExporter`                                                                                                                                                                     | `dev.nthings.otlp4j.exporter`       |
| `Transforms`, `BatchingProcessor`, `OverflowPolicy`                                                                                                                                | `dev.nthings.otlp4j.processor`      |
| `Connectors`, `FailurePolicy`                                                                                                                                                      | `dev.nthings.otlp4j.connector`      |
| Configuration: `ClientConfig`, `ServerConfig`, `Tls`, `Compression`, `RetryPolicy`                                                                                                 | `dev.nthings.otlp4j.config`         |
| Transport SPI: `OtlpClient`, `OtlpServer`, `Dispatchers`                                                                                                                           | `dev.nthings.otlp4j.spi`            |
| Domain records: `TracesData`, `MetricsData`, `LogsData`, `ProfilesData`, `Resource`, `Attributes`, `AttributeValue`, `Span`, `Metric`, `LogRecord`, `Exemplar`, `ConsumeResult`, … | `dev.nthings.otlp4j.model`          |

## If you know OpenTelemetry Go

`otlp4j` sits closer to a small Collector-style gateway than a single SDK exporter, so concepts map across rather than one-to-one:

| OpenTelemetry Go                                                                      | otlp4j                                                                                                                   |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `otlptracegrpc` / `otlpmetricgrpc` / `otlploggrpc` (one package per signal+transport) | One `OtlpGrpcExporter`; pick a signal facet — `exporter.traces()`, `.metrics()`, `.logs()`, `.profiles()`.               |
| `go.opentelemetry.io/proto/otlp/...` generated protobuf                               | Proto-free immutable records in `dev.nthings.otlp4j.model` (`TracesData`, `MetricsData`, `LogsData`, `ProfilesData`, …). |
| Collector `consumer` (`ConsumeTraces(ctx, td) error`)                                 | `TraceSink` (a `Sink<TracesData>`) returning `CompletionStage<ConsumeResult<TracesData>>`.                               |
| Collector `component` lifecycle (`Start`/`Shutdown(ctx)`)                             | `OtlpGrpcReceiver.start()`, `PipelineHandle.shutdown(Duration)`, `Lifecycle`.                                            |
| Collector OTLP receiver                                                               | `OtlpGrpcReceiver` plus its per-signal `Source`s.                                                                        |
| `exporterhelper` queue + retry + timeout                                              | `BatchingProcessor` (queue), `RetryPolicy` (transport retry), per-request `timeout(...)`.                                |
| Functional options (`WithEndpoint`, `WithTimeout`, `WithInsecure`)                    | Builder methods (`endpoint(...)`, `timeout(...)`); the endpoint scheme decides plaintext vs TLS.                         |
| `consumer.Capabilities{MutatesData}`                                                  | Not needed — model records are immutable, so fan-out shares them without copying.                                        |

Two deliberate differences: delivery is asynchronous (`CompletionStage<ConsumeResult<T>>`) with no per-call `context.Context`, and OTLP is carried over gRPC or HTTP with binary protobuf only (no `http/json`). Pick the transport by class — `OtlpGrpcExporter`/`OtlpGrpcReceiver` (port 4317) or `OtlpHttpExporter`/`OtlpHttpReceiver` (port 4318); the builders and pipeline wiring are identical. See [Sinks and results](#sinks-and-results) for the partial-success/retry mapping.

## Domain model

Each signal preserves OTLP's resource and instrumentation-scope grouping while providing a flattened accessor plus allocation-free traversal and count helpers:

| Batch          | Grouping                                         | Flattened accessor | `forEach` helper             | Count helper       |
| -------------- | ------------------------------------------------ | ------------------ | ---------------------------- | ------------------ |
| `TracesData`   | `ResourceSpans` → `ScopeSpans` → `Span`          | `spans()`          | `forEachSpan(Consumer)`      | `spanCount()`      |
| `MetricsData`  | `ResourceMetrics` → `ScopeMetrics` → `Metric`    | `metrics()`        | `forEachMetric(Consumer)`    | `dataPointCount()` |
| `LogsData`     | `ResourceLogs` → `ScopeLogs` → `LogRecord`       | `logRecords()`     | `forEachLogRecord(Consumer)` | `logRecordCount()` |
| `ProfilesData` | `ResourceProfiles` → `ScopeProfiles` → `Profile` | `profiles()`       | `forEachProfile(Consumer)`   | `profileCount()`   |

Each flattened accessor (`spans()`, `metrics()`, `logRecords()`, `profiles()`) walks the resource/scope grouping and allocates a fresh list on every call, so bind it to a local rather than re-calling it in a loop or on a hot path. On hot paths prefer the `forEach…` helper to visit items in the same order without the intermediate list, or the count helper to size a batch without flattening it — these are what batching and the count connectors use. (`MetricsData`'s count helper, `dataPointCount()`, counts nested data points — the meaningful OTLP item count for metrics — rather than the number of `Metric` objects.)

Records copy incoming lists and are safe to share between fan-out peers. `Attributes` and the sealed `AttributeValue` hierarchy represent OTLP values. Builders are available for `Attributes`, `Span`, `Metric`, and `LogRecord`, plus the metric data points (`NumberPoint`, `HistogramPoint`, `ExponentialHistogramPoint`) and `Exemplar`; `NumberPoint`, `Exemplar`, and `SummaryPoint` also offer `of(...)` factories for the common case. These builders are batch/model-construction helpers for OTLP payloads in the pipeline, not application instrumentation APIs. The remaining records use canonical constructors. To avoid hand-nesting the resource/scope wrappers, each signal type has an `of(resource, scope, items)` factory, and `Resource.of(...)` / `InstrumentationScope.of(...)` cover the common cases:

```java
var batch = TracesData.of(Resource.of(attributes), InstrumentationScope.of("my.lib", "1.0"), spans);
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

`Accepted` and `Partial` are normal OTLP responses: `Accepted` leaves `partial_success` unset, and `Partial` carries the rejected count and message. A whole-batch `Rejected` is **not** a partial success (encoding it as `rejected_*=0` would read to the client as "all accepted"), so the bundled gRPC transport maps it to a gRPC error instead of a response message. `Rejected` states its retry intent explicitly; a diagnostic `cause` is optional on either disposition:

- A **retryable** `Rejected` → gRPC `UNAVAILABLE` / HTTP `503`; use it for transient back-pressure such as a full queue or a briefly unreachable downstream. Build it with `ConsumeResult.retryable(message)` or `ConsumeResult.retryable(message, cause)`.
- A **permanent** `Rejected` → gRPC `INTERNAL` / HTTP `500`; use it for a permanent fault such as a policy or validation failure that would reject the same batch every time. Build it with `ConsumeResult.permanent(message)` or `ConsumeResult.permanent(message, cause)`.

Use an exception or exceptionally completed stage for a transport-level failure.

## Receive

`OtlpGrpcReceiver` defaults to plaintext `localhost:4317`; call `.setTls(Tls.custom(cert, key, …))` on the builder to serve TLS. Port `0` selects an ephemeral port, available through `port()` after `start()`. A wildcard `bindHost` (empty, `0.0.0.0`, or `::`) binds every interface; any other host binds that specific interface, so `127.0.0.1` yields a fixed loopback-only receiver.

`OtlpHttpReceiver` is the OTLP/HTTP counterpart with the same builder, defaulting to `localhost:4318`. It serves the standard signal paths (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`), inflates gzip request bodies, and — lacking a per-connection concurrency knob — bounds concurrency through `setServerExecutor(...)` (a virtual-thread-per-request executor by default).

The receiver builder exposes the receiver-hardening knobs the bundled server applies directly (or set them on a `ServerConfig` and pass it through `transport(...)`), all defaulting to gRPC's own behaviour:

| Builder knob                         | Default                       | Effect                                                                                     |
| ------------------------------------ | ----------------------------- | ------------------------------------------------------------------------------------------ |
| `setMaxInboundMessageSizeBytes`      | 4 MiB                         | Caps a single decoded export request; guards against memory-exhausting oversized requests. |
| `setMaxConcurrentCallsPerConnection` | `0` (gRPC default, unlimited) | A positive value caps in-flight calls per connection.                                      |
| `setHandshakeTimeout`                | 20s                           | Bounds the transport/TLS handshake only — not a slow request body or an idle connection.   |
| `setServerExecutor`                  | `null` (gRPC's own executor)  | Supply a bounded pool to cap admitted concurrent work.                                     |

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

Compression is asymmetric: the server transparently decodes gzip request bodies via gRPC's default decoder and exposes no server compression knob (response compression is intentionally not configured).

**Production receiver checklist.** The default loopback bind is convenient locally. Before exposing a receiver in production:

- **Bind host** — keep the default loopback bind or choose a specific interface unless you mean to listen on every interface with a wildcard host.
- **TLS** — serve TLS with `.setTls(Tls.custom(cert, key, trust))`; require client certs (mTLS) on untrusted networks.
- **Inbound size cap** — call `setMaxInboundMessageSizeBytes` to bound a single decoded request.
- **Concurrency cap** — call `setMaxConcurrentCallsPerConnection` and/or a bounded `setServerExecutor` to cap admitted work.
- **Handshake timeout** — keep `setHandshakeTimeout` tight to shed stalled TLS handshakes.
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
PipelineHandle subscription = Pipeline.from(receiver.traces())
        .filter(traces -> !traces.spans().isEmpty())
        .to(report);
```

Attaching a second consumer to an already-attached source throws `IllegalStateException`. Closing its subscription releases the slot. An unattached signal is rejected retryably (`UNAVAILABLE` for gRPC, `503` for HTTP). Call `receiver.metrics().discard()` or the equivalent per-signal source when accepting and dropping that signal is intentional.

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

A fan-out to exporter facets drains each exporter automatically — the facets are collected as fan-out peers:

```java
var primary = OtlpGrpcExporter.to("collector-a", 4317);
var secondary = OtlpGrpcExporter.to("collector-b", 4317);

var subscription = Pipeline.from(receiver.traces())
        .filter(traces -> !traces.spans().isEmpty())
        .to(FanOut.of(primary.traces(), secondary.traces()));
// subscription.shutdown(timeout) drains primary and secondary within one budget.
```

For a resource the pipeline can't reach as a terminal or peer — declare it on the stage with `owns(...)` (chainable) _before_ `.to(...)`. Ownership is declared on the stage, not per peer.

When destinations have independent speeds or failure modes, give each its own queue: front each exporter with its own `BatchingProcessor` so a slow or failing collector drains its own buffer without stalling the others. The batchers are `AutoCloseable` fan-out peers (auto-drained), and each exporter sitting behind one is hidden, so `owns(...)` it:

```java
var primaryQueue = BatchingProcessor.forTraces().downstream(primary.traces()).build();
var secondaryQueue = BatchingProcessor.forTraces().downstream(secondary.traces()).build();

var subscription = Pipeline.from(receiver.traces())
        .owns(primary)
        .owns(secondary)
        .to(FanOut.of(primaryQueue, secondaryQueue));
```

Available built-in transforms are span and log-record filters plus per-signal resource-attribute setters. Implement `Transform<T>` for other synchronous one-to-one rewrites.

`FanOut.of(...)` is the one fan-out spelling: a reusable `Sink` that a stage terminates at with `.to(...)`. Peers run concurrently; the result is rejected if any peer rejects, otherwise partial results use the largest rejection count.

To observe batches in-path, add an observing `Sink` as a fan-out peer: it runs concurrently with the terminal and shares the acknowledgement, so an observer that rejects also rejects the batch back to the sender. An identity `Transform` observes inline instead, but an observer that throws becomes a permanent rejection unless it catches its own exceptions. For demand-aware streaming with bounded buffers that stays outside the acknowledgement path, use the receiver's `TelemetryTap`.

The routing concepts, contrasted:

| Concept                          | Cardinality            | Changes signal? | Owns downstream lifecycle?                 |
| -------------------------------- | ---------------------- | --------------- | ------------------------------------------ |
| `Transform` (via `Transforms`)   | 1 → 1                  | no              | no                                         |
| Count sinks (via `Connectors`)   | 1 → 1                  | yes             | yes — cascades to its downstream           |
| `BatchingProcessor`              | N → 1 (buffered)       | no              | no — front its downstream with `owns(...)` |
| `TelemetryTap` (on the receiver) | observe (demand-aware) | no              | n/a                                        |

## Batch

Create a signal-specific batcher and attach it as the terminal consumer:

```java
var batcher = BatchingProcessor.forTraces()
        .downstream(exporter.traces())
        .flushThreshold(512)
        .queueCapacity(2048)
        .overflowPolicy(OverflowPolicy.DROP_NEWEST)
        .build();

var subscription = Pipeline.from(receiver.traces()).to(batcher);
```

Both knobs count **batches**, not telemetry items: `flushThreshold` is the queue depth that triggers a drain, and `queueCapacity` is the hard cap (overflow follows the `OverflowPolicy`). The four factories are `forTraces`, `forMetrics`, `forLogs`, and `forProfilesUnsafe`. `queued()` reports buffered batches, not telemetry item count. `droppedCount()` counts dropped batches. Besides reaching the `flushThreshold`, a periodic one-second timer flushes whatever has queued, so a partly filled batch never waits indefinitely.

Overflow behavior (the `OverflowPolicy`):

| Policy        | Result                                                                            |
| ------------- | --------------------------------------------------------------------------------- |
| `DROP_OLDEST` | Evict the oldest queued batch and accept the new batch                            |
| `DROP_NEWEST` | Drop the new batch and return `Partial` with its item count (`Accepted` if empty) |
| `BLOCK`       | Block until queue space is available                                              |
| `FAIL`        | Drop the new batch and return `Rejected`                                          |

`forceFlush` drains the current queue. `shutdown` stops the timer and drains once; the processor then rejects new batches. A pipeline subscription closes a directly attached batcher.

> [!WARNING]
>
> Profiles batching is constrained by the OTLP profile dictionary. A `ProfilesData` batch carries an opaque, batch-level `ProfilesDictionary` that each profile references by index, so merging is only lossless when the batches agree on that dictionary (a shared, or single non-empty, dictionary). `forProfilesUnsafe()` merges only same-dictionary batches; a flush that drains profiles carrying **distinct non-empty dictionaries** fails — the merge throws and the whole drained batch surfaces as a `BatchDeliveryException` from `forceFlush`/`shutdown`, since re-indexing every reference is out of scope. Forward profiles 1:1 (do not batch them) unless every producer shares one dictionary.

## Export

`OtlpGrpcExporter` defaults to plaintext `localhost:4317` with a ten-second deadline per request. It owns one client channel and exposes a sink facet for each signal. The endpoint can be set as a single `setEndpoint(url)` URL or as `setEndpoint(host, port)`; TLS, authentication headers, gzip compression, and retries are available directly on the builder (`setTls`, `addHeader`/`setHeaders`, `setCompression`, `setRetryPolicy`). Retries are **on by default** (`RetryPolicy.getDefault()` — five attempts, 1s→5s, 1.5×); pass `RetryPolicy.none()` to opt out. `setHeaders(Supplier<Map<String,String>>)` supplies headers evaluated per export (for a rotating bearer token), overlaid on the static headers (the supplier wins per key). `setTls` accepts PEM file paths, in-memory PEM bytes (`Tls.custom(byte[]…)` / `Tls.trust(byte[])`), or a caller-built `SSLContext` (`Tls.sslContext(context, trustManager)`). Pass a fully built `ClientConfig` through `transport(...)` only when you want to replace the whole config at once.

`OtlpHttpExporter` is the OTLP/HTTP counterpart with the identical builder, defaulting to `localhost:4318`. It POSTs each signal's binary protobuf to its standard path (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`) as `application/x-protobuf`; the scheme follows `setTls` (`http`/`https`), `setCompression(GZIP)` sets `Content-Encoding: gzip`, and `setRetryPolicy` drives exponential-backoff retries over retryable statuses (408/429/502/503/504). An endpoint **path prefix** is applied: a collector behind `https://host/otlp` is reached at `/otlp/v1/traces`, set via the `setEndpoint(url)` / `OTEL_EXPORTER_OTLP_ENDPOINT` URL or the HTTP builder's `setPath(...)`. (gRPC ignores the path and uses the authority only.) `setConnectTimeout(Duration)` bounds connection setup, falling back to the request timeout when unset; it applies to OTLP/HTTP only (gRPC has no separate connect timeout and ignores it).

By default the exporter reads no environment — construction is fully explicit and deterministic. Opt in to the standard general OTLP variables with `fromEnvironment()` on the exporter or `ClientConfig` builder, or the static `OtlpGrpcExporter.fromEnvironment()` / `OtlpHttpExporter.fromEnvironment()` shorthand. It reads each variable only when present; environment values are **lowest precedence and order-independent** — applied at `build()` only where you did not set the field explicitly, so call order does not matter. Malformed values throw. Only general (non-signal-specific) variables are read:

| Setting                     | Builder method                                         | Environment variable                            | otlp4j default                                          | Notes                                                                                                                                                                                                                                     |
| --------------------------- | ------------------------------------------------------ | ----------------------------------------------- | ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Endpoint host/port/scheme   | `.setEndpoint(url)` / `.setEndpoint(host, port)` / `.setTls` | `OTEL_EXPORTER_OTLP_ENDPOINT`              | gRPC `localhost:4317`, HTTP `localhost:4318`, plaintext | A URL; `http` is plaintext, `https` selects TLS. gRPC uses the authority as-is; HTTP appends the standard `/v1/<signal>` paths to any URL path prefix. A URL without a port keeps the exporter's protocol default (4317 gRPC, 4318 HTTP). |
| Endpoint path prefix (HTTP) | `.setPath(String)` (HTTP builder)                      | path component of `OTEL_EXPORTER_OTLP_ENDPOINT` | none                                                    | Prepended to `/v1/<signal>` — `https://host/otlp` → `/otlp/v1/traces`. Normalized (a bare `/` means none); gRPC ignores it.                                                                                                               |
| Request timeout             | `.setTimeout(Duration)`                                | `OTEL_EXPORTER_OTLP_TIMEOUT`                    | `10s`                                                   | Integer milliseconds; must be > 0.                                                                                                                                                                                                        |
| Headers                     | `.addHeader(k, v)` / `.setHeaders(map)`                | `OTEL_EXPORTER_OTLP_HEADERS`                    | none                                                    | `k=v,k2=v2`; values are percent-decoded (`+` stays literal). `.setHeaders(map)` replaces all static headers; explicit `.addHeader(k, v)`/`.setHeaders(map)` keys win, and the env variable fills only keys you did not set. `setHeaders(Supplier<Map>)` (no env form) supplies headers per export, overlaid on the static headers. |
| Compression                 | `.setCompression(Compression)` / `.setCompression(String)` | `OTEL_EXPORTER_OTLP_COMPRESSION`            | `NONE`                                                  | `gzip` or `none`.                                                                                                                                                                                                                         |
| Insecure (plaintext)        | `.setTls(Tls.disabled())`                              | `OTEL_EXPORTER_OTLP_INSECURE`                   | TLS off                                                 | `true`/`false`. Consulted only when no endpoint URL sets the scheme (a present endpoint's `http`/`https` wins); `true` forces plaintext, overriding the certificate variables.                                                            |
| Server CA / trust           | `.setTls(Tls.trust(path))`                             | `OTEL_EXPORTER_OTLP_CERTIFICATE`                | system trust when TLS is on                             | Used when the endpoint is `https`, or — with no endpoint set — as an independent input that turns TLS on. Ignored on an `http` endpoint.                                                                                                  |
| Client cert (mTLS)          | `.setTls(Tls.custom(cert, key, trust))`               | `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE`         | none                                                    | Requires the client key; honoured with an `https` endpoint or, with no endpoint, as an independent input; ignored on an `http` endpoint.                                                                                                  |
| Client key (mTLS)           | `.setTls(Tls.custom(cert, key, trust))`               | `OTEL_EXPORTER_OTLP_CLIENT_KEY`                 | none                                                    | Requires the client certificate.                                                                                                                                                                                                          |

Deliberately not read: `OTEL_EXPORTER_OTLP_PROTOCOL` (the protocol is chosen by which exporter class you instantiate, `OtlpGrpcExporter` vs `OtlpHttpExporter`, not by env). Signal-specific overrides such as `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` are out of scope: one exporter drives all four signals over a single connection, so a per-signal endpoint would need its own exporter — instantiate one `OtlpGrpcExporter`/`OtlpHttpExporter` per destination and route each signal to it through the pipeline. The same `fromEnvironment()` applies to `OtlpHttpExporter`, except a portless endpoint URL keeps the HTTP default port 4318.

```java
// Env fills the gaps; the explicit endpoint wins over OTEL_EXPORTER_OTLP_ENDPOINT
// regardless of the order these are called.
var exporter = OtlpGrpcExporter.builder()
        .fromEnvironment()
        .setEndpoint("collector.example.com", 4317)
        .build();
```

```java
try (var exporter = OtlpGrpcExporter.builder()
        .setEndpoint("collector.example.com", 4317)
        .setTimeout(Duration.ofSeconds(5))
        .build()) {
    ConsumeResult<TracesData> result = exporter.traces()
            .consume(traces)
            .toCompletableFuture()
            .join();
}
```

A hardened exporter — an `https` URL endpoint selecting system-trust TLS (or `.setTls(Tls.custom(cert, key, trust))` for mTLS), a bearer header, gzip, and exponential retry with a backoff multiplier:

```java
var exporter = OtlpGrpcExporter.builder()
        .setEndpoint("https://collector.example.com:4317")
        .addHeader("authorization", "Bearer " + token)
        .setCompression(Compression.GZIP)
        .setRetryPolicy(RetryPolicy.builder()
                .setMaxAttempts(5)
                .setInitialBackoff(Duration.ofSeconds(1))
                .setMaxBackoff(Duration.ofSeconds(30))
                .setBackoffMultiplier(1.5)
                .build())
        .setTimeout(Duration.ofSeconds(10))
        .build();
```

`OtlpGrpcExporter`/`OtlpHttpExporter` are builder entry points; `build()` (and the `to(...)`/`fromEnvironment()` shortcuts) returns an `OtlpExporter`, the multi-signal export contract (the counterpart to `Receiver` on the ingest side). The exporter itself owns the channel. Its `traces()`, `metrics()`, `logs()`, and `profiles()` facets are lifecycle-bearing views that deliver to the client and share that one channel, so attaching any facet to a pipeline drains the whole exporter on shutdown with nothing to register. As a `Lifecycle`, the exporter receives the pipeline's _remaining_ shared deadline, and its shutdown is cancellation-aware. `forceFlush` is a no-op today (the client holds no buffer). If you use a facet outside a pipeline (calling `consume` directly), close the exporter yourself. As a backstop, an exporter that is garbage-collected without a `shutdown()` or `close()` logs a warning — a leaked channel surfaces in the logs instead of silently.

Implement `Sink<T>` plus `Lifecycle` for a custom typed terminal with flush and shutdown hooks. Implement the lower-level client or server SPI when replacing the wire transport.

## Connect signals

The bundled count sinks consume one signal and emit a derived metric to a configured downstream `MetricSink`. `Connectors.spanCount` returns a `TraceSink`; `Connectors.logRecordCount` returns a `LogSink`. Wire them in like any other sink — as a terminal or a fan-out peer:

```java
var spanCounter = Connectors.spanCount(exporter.metrics());
var logCounter = Connectors.logRecordCount(exporter.metrics());

// Or fail the input batch when the derived metric is not delivered:
var strictSpanCounter = Connectors.spanCount(exporter.metrics(), FailurePolicy.FAIL);
```

A count sink cascades its lifecycle to the downstream `MetricSink` it wraps, so attaching the connector as a terminal or fan-out peer drains that downstream exporter automatically — no `Stage.owns(...)` needed (see [Lifecycle](#lifecycle)).

The built-ins emit `otlp4j.connector.span.count` and `otlp4j.connector.log.record.count`, each as a monotonic delta sum whose window runs from the previous flush (so the series carries a real per-series start time). A configurable `FailurePolicy` decides how a downstream metric failure maps back onto the input result; the no-policy `spanCount`/`logRecordCount` overloads default to `BEST_EFFORT`, and `spanCount(downstream, policy)` / `logRecordCount(downstream, policy)` set it explicitly:

| Policy                  | Downstream `Partial`/`Rejected` | Input result                                                          |
| ----------------------- | ------------------------------- | --------------------------------------------------------------------- |
| `BEST_EFFORT` (default) | logged                          | `Accepted` — derived telemetry never fails the originating request    |
| `FAIL`                  | logged                          | `Rejected`, so the caller learns the derived metric was not delivered |

An exceptionally completed downstream stage still propagates either way (a metric rejection cannot be relabeled as a trace or log rejection).

## Observe live traffic

Every receiver has independent JDK `Flow.Publisher` streams:

```java
receiver.tap().setOptions(
        new TapOptions(OverflowPolicy.DROP_OLDEST, 512));

Flow.Publisher<TracesData> traces = receiver.tap().traces();
Flow.Publisher<Telemetry> allSignals = receiver.tap().all();
```

`all()` emits `Telemetry.Traces`, `.Metrics`, `.Logs`, or `.Profiles`. Defaults are a 256-batch buffer per subscription and `DROP_OLDEST`. Options apply when a subscriber attaches. `droppedCount()` aggregates tap drops for the receiver.

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
        // stream failed — e.g. the FAIL overflow policy terminated the stream on buffer overflow
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

- `OtlpClient` (export) exposes `exportTraces`/`exportMetrics`/`exportLogs`/`exportProfiles`, each returning `CompletionStage<ConsumeResult<T>>`. Wrap your client in an exporter that publishes the per-signal `Sink` facets (`traces()`, `metrics()`, …) and a `shutdown`/`forceFlush` lifecycle.
- `OtlpServer` (receive) is constructed with a `ServerConfig` and a `Dispatchers` record — the per-signal functions the server invokes as it decodes requests. Build the `Dispatchers`, hand them to your server, and expose one `Source` per signal plus a `TelemetryTap`.

For a custom *terminal* rather than a new wire protocol — somewhere telemetry lands that isn't OTLP at all — implement `Sink<T>` plus `Lifecycle` (see [Export](#export)) instead of the transport SPI.

Configuration arrives through `ClientConfig` and `ServerConfig`. The bundled clients honour host, port, timeout, TLS, headers, compression, and retry; the servers honour their port, `bindHost`, TLS, and the receiver-hardening limits (inbound size cap, per-connection concurrency, handshake timeout, executor).

## Lifecycle

Shut the receiver down first, then the subscription. `receiver.shutdown(timeout)` stops the receiver accepting new requests and drains the in-flight ones through the still-live pipeline and exporter; closing the subscription afterward detaches the source instantly and drains everything it references (directly attached processors, exporter facets, and `AutoCloseable` fan-out peers) within one shared deadline. Draining the receiver first keeps live senders reaching a working exporter for the whole window — the topological order the Collector uses, stopping receivers before the exporters they feed.

The subscription drains every terminal and fan-out peer it can see. Exporter facets carry their exporter's lifecycle, so attaching one drains the whole exporter with nothing to register; a count connector likewise cascades its shutdown to the downstream metric sink it wraps, so attaching it drains that downstream too. When two subscriptions attach facets of the _same_ exporter, its channel closes only once both have shut down. Use `Stage.owns(resource)` only for a resource the pipeline _cannot_ see — one hidden behind a bare lambda terminal, or an exporter fronted by a `BatchingProcessor` (which drains its own queue but does not close its downstream):

```java
// A bare-lambda terminal hides the exporter it delegates to — declare it so the subscription drains it.
var sideExporter = OtlpGrpcExporter.to("side-collector", 4317);
var subscription = Pipeline.from(receiver.traces())
        .owns(sideExporter)
        .to(traces -> sideExporter.traces().consume(traces));
```

`Stage.owns(...)` is chainable and must be declared on the stage _before_ `.to(...)`. A resource the pipeline never drains surfaces as a warning logged when the exporter is garbage-collected, not a silent channel leak.

Use `shutdown(Duration)` when completion matters. The convenience `close()` methods drain gracefully within a fixed ten-second grace period (`Lifecycle.DEFAULT_GRACE_PERIOD`), applied uniformly by every `Lifecycle` — receiver, exporter, batching processor, and subscription; call `Receiver.shutdownNow()` for an immediate stop.


## Thread-safety and nullness

**Thread-safety.** The runtime types are safe to share across threads; the _builders_ are not.

| Type                                          | Contract                                                                                                                                                                                                                                                           |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `OtlpGrpcReceiver` / `OtlpHttpReceiver`       | Thread-safe after `start()`; dispatch incoming requests to sinks concurrently. The `…Receiver.Builder` is single-threaded — configure on one thread, then `build()`.                                                                                               |
| `OtlpGrpcExporter` / `OtlpHttpExporter`       | Thread-safe: a single exporter owns one client/channel and may be called from many threads; each facet's `consume` is concurrency-safe. The builder is single-threaded. Attaching a facet to a pipeline drains the exporter on shutdown. |
| `BatchingProcessor`                           | Thread-safe: `consume`, `forceFlush`, and `shutdown` may be called concurrently (queue + counters are concurrent). `shutdown` is idempotent and then rejects new batches.                                                                                          |
| `PipelineHandle` (from `Pipeline.from(...)`)  | `shutdown`/`forceFlush` are safe to call concurrently and idempotently. Build the pipeline on one thread.                                                                                                                                                          |
| `TelemetryTap`                                | Thread-safe but not transactional: `setOptions` affects subscribers that attach afterward. Each `Flow.Subscriber` manages its own demand.                                                                                                                          |
| Model records (`TracesData`, `Attributes`, …) | Immutable and freely shareable; fan-out peers share them without copying. Their builders are single-threaded.                                                                                                                                                      |

`close()` blocks on the drain — it calls `shutdown(...).join()` — so do not call it from a pipeline or completion thread the drain itself needs; that thread can stall until the deadline. From inside async code, call `shutdown(Duration)` and compose the returned stage instead.

**Nullness.** Every public package is `@NullMarked`: parameters, return types, and fields are non-null unless explicitly annotated `@Nullable`. The intentionally nullable spots in the public surface are:

- **Receiver builder signal callbacks** — `onTraces`/`onMetrics`/`onLogs`/`onProfiles` are optional; an unset signal slot is rejected retryably unless the source is explicitly attached or discarded.
- **`serverExecutor`** (receiver builder / `ServerConfig`) — `null` keeps gRPC's own executor.
- **`Tls.custom(cert, key, trustFile)`** — a `null` `trustFile` falls back to system trust. Besides PEM file paths, `Tls` accepts in-memory PEM bytes (`Tls.custom(byte[]…)` / `Tls.trust(byte[])`) and a caller-built `SSLContext` (`Tls.sslContext(SSLContext, X509TrustManager)`). The `SSLContext` variant is used verbatim on OTLP/HTTP (a client certificate it carries is presented for mTLS); on OTLP/gRPC only the `X509TrustManager` is used for server verification, since gRPC has no `SSLContext` door — use `Tls.custom(...)` (paths or bytes) for gRPC client certificates. Both servers reject `Tls.sslContext` and `Tls.systemTrust`, which carry no server certificate.
- **`ConsumeResult.Rejected.cause`** — an optional diagnostic throwable, orthogonal to retryability; `null` when no cause is available. See [Sinks and results](#sinks-and-results).
- **Point values** — `NumberPoint`/`Exemplar` value may be `null` for a value-unset point.
- **`Attributes.get(key)`** (and typed `getString`, …) — returns `null` on a lookup miss.

`BatchingProcessor.Builder.downstream` must be set before `build()`; building without it throws rather than accepting `null`.
