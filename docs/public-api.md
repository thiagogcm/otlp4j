# Public API

Application code normally depends on `otlp4j-api`, which transitively exposes `otlp4j-model`. Add `otlp4j-transport` at runtime to use the bundled OTLP/gRPC provider. Packages in the transport and generated proto modules are implementation details.

## Domain model

Each signal preserves OTLP's resource and instrumentation-scope grouping while providing a flattened accessor:

| Batch | Grouping | Flattened accessor |
| --- | --- | --- |
| `TraceData` | `ResourceSpans` → `ScopeSpans` → `Span` | `spans()` |
| `MetricsData` | `ResourceMetrics` → `ScopeMetrics` → `Metric` | `metrics()` |
| `LogsData` | `ResourceLogs` → `ScopeLogs` → `LogRecord` | `logRecords()` |
| `ProfilesData` | `ResourceProfiles` → `ScopeProfiles` → `Profile` | `profiles()` |

Records copy incoming lists and are safe to share between fan-out peers. `Attributes` and the sealed `AttributeValue` hierarchy represent OTLP values. Builders are available for `Attributes`, `Span`, `Metric`, and `LogRecord`; the remaining records use canonical constructors.

`Metric.Data` covers gauge, sum, histogram, exponential histogram, and summary. Profiles are marked `@Experimental` and expose only top-level metadata.

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

`OtlpGrpcReceiver` defaults to plaintext `0.0.0.0:4317`; pass a `ServerTransportConfig` with `Tls.Custom(cert, key, …)` through `transport(...)` to serve TLS. Port `0` selects an ephemeral port, available through `port()` after `start()`.

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
        .transform(Transforms.setTraceResourceAttribute(
                "service.namespace", AttributeValue.of("store")))
        .filter(traces -> !traces.spans().isEmpty())
        .branch()
            .fanOut(exporter.traces())
            .fanOut(spanCounter)
        .join();
```

Available built-in transforms are span and log-record filters plus per-signal resource-attribute setters. Implement `Transform<T>` for other synchronous one-to-one rewrites.

`FanOut.of(...)` is the direct alternative to the branch builder. Peers run concurrently; the result is rejected if any peer rejects, otherwise partial results use the largest rejection count.

`Pipeline.tap(observer)` is a fire-and-forget side effect inside the main graph. For demand-aware streaming with bounded buffers, use the receiver's `TelemetryTap` instead.

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

The exporter itself owns the channel. Its `traces()`, `metrics()`, `logs()`, and `profiles()` facets do not own or close it. `forceFlush` currently completes immediately because the client has no exporter-side buffer.

Implement `Exporter<T>` for a custom typed terminal with flush and shutdown hooks. Implement the lower-level client or server SPI when replacing the wire transport.

## Connect signals

`Connector<I,O>` consumes one signal and emits another to its configured downstream consumer:

```java
var spanCounter = new SpanCountConnector(exporter.metrics());
var logCounter = new LogRecordCountConnector(exporter.metrics());
```

The built-ins emit `otlp4j.connector.span.count` and `otlp4j.connector.log.record.count`. They always accept the input batch after the downstream stage completes; a downstream partial result or rejection is logged because a metric rejection cannot be relabeled as a trace or log rejection. An exceptionally completed downstream stage still propagates.

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

Helpers select the first provider returned by `ServiceLoader`; avoid ambiguous provider sets. Configuration arrives through `ServerTransportConfig` or `ClientTransportConfig`. The bundled client honours host, port, timeout, TLS, headers, compression, and retry; the server honours its port and TLS (it does not yet apply `bindHost`).

## Shutdown order

Stop ingestion before closing downstream resources:

1. Shut down the pipeline subscription to detach the source and drain directly owned processors.
2. Close exporter instances and any connector-owned downstream resources explicitly.
3. Shut down the receiver.

Use `shutdown(Duration)` when completion matters. Convenience `close()` methods use fixed defaults, while `Receiver.close()` performs an immediate shutdown.
