# Public API

Application code normally depends on `otlp4j-api`, which transitively exposes `otlp4j-model`. Add `otlp4j-transport` at runtime to use the bundled OTLP/gRPC provider. Packages in the transport and generated proto modules are implementation details.

Everything lives under the `dev.nthings.otlp4j` root. The types you import most often:

| Type(s) | Package |
| --- | --- |
| `OtlpGrpcReceiver`, `Receiver`, `TelemetryTap` | `dev.nthings.otlp4j.receiver` |
| `OtlpGrpcExporter`, `Exporter` | `dev.nthings.otlp4j.exporter` |
| `Pipeline`, `Source`, `Subscription`, `Consumer` (+ `TraceConsumer` … aliases), `Transform`, `FanOut`, `ConsumeResult`, `Telemetry`, `Drainable`, `Flushable` | `dev.nthings.otlp4j.pipeline` |
| `Transforms`, `BatchingProcessor`, `DropPolicy` | `dev.nthings.otlp4j.processor` |
| `Connector`, `Connectors`, `FailurePolicy` | `dev.nthings.otlp4j.connector` |
| Transport SPI: `OtlpClient(Provider)`, `OtlpServer(Provider)`, `ClientTransportConfig`, `ServerTransportConfig`, `Tls`, `Compression`, `RetryPolicy` | `dev.nthings.otlp4j.spi` |
| Domain records: `TraceData`, `MetricsData`, `LogsData`, `ProfilesData`, `Resource`, `Attributes`, `AttributeValue`, `Span`, `Metric`, `LogRecord`, `Exemplar`, … | `dev.nthings.otlp4j.model` |

## Domain model

Each signal preserves OTLP's resource and instrumentation-scope grouping while providing a flattened accessor:

| Batch | Grouping | Flattened accessor |
| --- | --- | --- |
| `TraceData` | `ResourceSpans` → `ScopeSpans` → `Span` | `spans()` |
| `MetricsData` | `ResourceMetrics` → `ScopeMetrics` → `Metric` | `metrics()` |
| `LogsData` | `ResourceLogs` → `ScopeLogs` → `LogRecord` | `logRecords()` |
| `ProfilesData` | `ResourceProfiles` → `ScopeProfiles` → `Profile` | `profiles()` |

Records copy incoming lists and are safe to share between fan-out peers. `Attributes` and the sealed `AttributeValue` hierarchy represent OTLP values. Builders are available for `Attributes`, `Span`, `Metric`, and `LogRecord`; the remaining records use canonical constructors. To avoid hand-nesting the resource/scope wrappers, each signal type has an `of(resource, scope, items)` factory, and `Resource.of(...)` / `InstrumentationScope.of(...)` cover the common cases:

```java
var batch = TraceData.of(Resource.of(attributes), InstrumentationScope.of("my.lib", "1.0"), spans);
```

`Attributes.toBuilder()` plus `Builder.putAll(...)` make "copy and add a key" a one-liner instead of a manual map walk.

`Metric.Data` covers gauge, sum, histogram, exponential histogram, and summary; it is `null` exactly when the wire metric set no data (`DATA_NOT_SET`), so null-check `Metric.data()` before use. Number, histogram, and exponential-histogram points each carry a `List<Exemplar>` (`filteredAttributes`, `epochNanos`, `value`, `spanId`, `traceId`) mapped in both directions; the list is empty when no exemplars were recorded.

`ProfilesData` is marked `@Experimental` and forwards profiles losslessly via opaque passthrough. Its top-level `Profile` metadata is best-effort inspection only; each `Profile` also carries `rawProfile` (the serialized proto `Profile`) and the batch carries `dictionary` (the serialized `ProfilesDictionary`), so the payload re-emits byte-for-byte. Both are opaque `byte[]` — treat them as opaque and do not mutate; their accessors return defensive clones.

## Consumers and results

`Consumer<T>` is the asynchronous pipeline contract:

```java
CompletionStage<ConsumeResult<T>> consume(T batch);
```

Prefer the signal aliases `TraceConsumer`, `MetricConsumer`, `LogConsumer`, and `ProfileConsumer` in declarations and extension APIs.

```java
TraceConsumer report = traces -> {
    System.out.println("spans=" + traces.spans().size());
    return ConsumeResult.acceptedStage();
};
```

`ConsumeResult<T>` has three variants:

- `Accepted<T>`: every item was accepted.
- `Partial<T>`: a positive item count was rejected; the rest was accepted.
- `Rejected<T>`: the complete batch was rejected.

Use an exception or exceptionally completed stage for a transport-level failure. A normal `Rejected` is still an OTLP response and is encoded as signal-specific partial-success data.

## Receive

`OtlpGrpcReceiver` defaults to plaintext `0.0.0.0:4317`; pass a `ServerTransportConfig` with `Tls.Custom(cert, key, …)` through `transport(...)` to serve TLS. Port `0` selects an ephemeral port, available through `port()` after `start()`. A wildcard `bindHost` (empty, `0.0.0.0`, or `::`) binds every interface; any other host binds that specific interface, so `127.0.0.1` yields a loopback-only receiver.

`ServerTransportConfig.builder()` also exposes the receiver-hardening knobs the bundled server applies, all defaulting to gRPC's own behaviour:

| Knob | Default | Effect |
| --- | --- | --- |
| `maxInboundMessageSizeBytes` | 4 MiB | Caps a single decoded export request; guards against memory-exhausting oversized requests. |
| `maxConcurrentCallsPerConnection` | `0` (gRPC default, unlimited) | A positive value caps in-flight calls per connection. |
| `handshakeTimeout` | 20s | Bounds the transport/TLS handshake only — not a slow request body or an idle connection. |
| `serverExecutor` | `null` (gRPC's own executor) | Supply a bounded pool to cap admitted concurrent work. |

Compression is asymmetric: the server transparently decodes gzip request bodies via gRPC's default decoder and exposes no server compression knob (response compression is intentionally not configured).

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

## Export

`OtlpGrpcExporter` defaults to plaintext `localhost:4317` with a ten-second deadline per request. It owns one client channel and exposes a consumer facet for each signal. TLS, authentication headers, gzip compression, and retries are configured by passing a fully built `ClientTransportConfig` through `transport(...)`.

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

The exporter itself owns the channel. Its `traces()`, `metrics()`, `logs()`, and `profiles()` facets are method references, so the pipeline cannot auto-discover the exporter behind them. Register it with `Stage.owns(exporter)` to hand its lifecycle to the subscription: it implements both `Drainable` and `Flushable`, so the subscription drains it on shutdown and reaches it on `forceFlush`. As a `Drainable` it receives the pipeline's *remaining* shared deadline, and its shutdown is cancellation-aware — a timeout interrupts the transport teardown rather than leaving it blocking past the deadline. `forceFlush` itself is a no-op today (the client holds no buffer) but is reachable once the exporter is owned. Without `owns`, close the exporter yourself.

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

## Supply another transport

Implement these pairs and register their providers with JPMS, `META-INF/services`, or both:

- `OtlpServerProvider` and `OtlpServer` for receiving;
- `OtlpClientProvider` and `OtlpClient` for exporting.

Helpers select the first provider returned by `ServiceLoader`; avoid ambiguous provider sets. Configuration arrives through `ServerTransportConfig` or `ClientTransportConfig`. The bundled client honours host, port, timeout, TLS, headers, compression, and retry; the server honours its port, `bindHost`, TLS, and the receiver-hardening limits (inbound size cap, per-connection concurrency, handshake timeout, executor).

## Shutdown order

Stop ingestion before closing downstream resources:

1. Shut down the pipeline subscription to detach the source and drain every owned resource — directly attached processors plus anything registered with `Stage.owns(...)` (e.g. an exporter), all within a single shared deadline.
2. Close any resources you did not hand to the subscription: exporter instances not registered via `owns`, and connector downstreams (which the subscription does not auto-discover).
3. Shut down the receiver.

Use `shutdown(Duration)` when completion matters. The convenience `close()` methods drain gracefully with a fixed ten-second default across the receiver, exporter, and subscription; call `Receiver.shutdownNow()` for an immediate stop.
