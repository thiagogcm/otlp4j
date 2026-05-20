# otlp4j SDK architecture standard

**Status:** current standard, accepted and implemented in the API surface.
**Updated:** 2026-05-20.

This document records the SDK architecture that replaced the earlier unified-consumer design. It is intentionally written as an implementation standard, not as a proposal.

## Design center

otlp4j is a Collector-shaped Java SDK for OTLP:

```text
Receiver source -> Pipeline transforms / filters / fan-out / batching / connectors -> Exporter
```

The SDK is not an OpenTelemetry storage backend, query engine, or retention layer. Its responsibility is typed ingest, transformation, fan-out, derived telemetry, and export while keeping generated proto/gRPC types outside application code.

## Current rules

1. **Public code uses typed model records.** `TraceData`, `MetricsData`, `LogsData`, and `ProfilesData` are the batch boundaries. Generated proto classes stay inside `otlp4j-proto` and `otlp4j-transport`.
2. **Consumers are per signal.** `TraceConsumer`, `MetricConsumer`, `LogConsumer`, and `ProfileConsumer` are single-method SAMs over `Consumer<T>`.
3. **Every consumer is asynchronous.** `Consumer<T>.consume(T)` returns `CompletionStage<ConsumeResult<T>>`.
4. **Acknowledgements are signal typed.** `ConsumeResult<T>` is sealed and carries accepted, partial, or rejected outcomes for one signal only.
5. **Receiver sources are explicit wiring points.** A `Receiver` exposes `Source<TraceData>`, `Source<MetricsData>`, `Source<LogsData>`, and `Source<ProfilesData>`.
6. **Fan-out is explicit.** A source accepts one attached consumer. Use `FanOut<T>` or `Pipeline.from(source).branch().fanOut(...).join()` when several peers need the same batch.
7. **Transforms are pure signal rewrites.** `Transform<T>` is the stateless processor shape. Built-ins live in `Transforms`.
8. **Batching is stateful and per signal.** `BatchingProcessor<T>` is queue-backed, timer-triggered, flushable, and configured through signal-specific builders.
9. **Connectors may change signal type.** `Connector<I,O>` consumes one signal and emits another; the built-in count connectors emit metrics from traces or logs.
10. **Live streaming is a side channel.** `TelemetryTap` exposes JDK `Flow.Publisher` streams and does not affect pipeline acknowledgements unless blocking tap back-pressure is deliberately selected.
11. **Transport stays behind SPI.** `OtlpGrpcReceiver` and `OtlpGrpcExporter` use `OtlpServerProvider` and `OtlpClientProvider`; applications do not import transport internals.

## Replaced API names

| Old shape                                  | Current standard                                                       |
| ------------------------------------------ | ---------------------------------------------------------------------- |
| `TelemetryConsumer`                        | `TraceConsumer`, `MetricConsumer`, `LogConsumer`, `ProfileConsumer`    |
| `ExportResult`                             | `ConsumeResult<T>`                                                     |
| `OtlpReceiver`                             | `OtlpGrpcReceiver` plus `Receiver` interface                           |
| `Pipeline.builder().process(...).into(...)` | `Pipeline.from(source).transform(...).to(...)` or `.branch().fanOut()` |
| `Processor` / `Processors`                 | `Transform<T>` / `Transforms` for stateless operations                 |
| `BatchProcessor`                           | `BatchingProcessor<T>`                                                 |
| `CountConnector`                           | `SpanCountConnector` and `LogRecordCountConnector`                     |
| One exporter-as-everything consumer         | `OtlpGrpcExporter` with per-signal facets                              |

## Core contracts

### Consumer and result

```java
@FunctionalInterface
public interface TraceConsumer extends Consumer<TraceData> {}

public interface Consumer<T> {
    CompletionStage<ConsumeResult<T>> consume(T batch);
}
```

`ConsumeResult<T>` has two merge modes:

- `fanOutMerge(...)` for the same batch sent to several peers. Rejection counts use the worst peer count.
- `sequentialMerge(...)` for one batch passing through sequential stages. Rejection counts can add because different stages may reject different items.

The type parameter prevents merging trace, metric, log, and profile rejection counters into a single meaningless value.

### Receiver and pipeline

```java
var receiver = OtlpGrpcReceiver.builder()
        .ephemeralPort()
        .build()
        .start();

var subscription = Pipeline.from(receiver.traces())
        .transform(Transforms.keepSpansWhere(span -> span.kind() == Span.Kind.SERVER))
        .branch()
            .fanOut(exporter.traces())
            .fanOut(new SpanCountConnector(exporter.metrics()))
        .join();
```

`Subscription` owns the attachment and shutdown path. Closing it detaches the source first, then drains lifecycle-aware leaves.

### Exporter

`OtlpGrpcExporter` owns one OTLP client and exposes four typed consumers:

```java
try (var exporter = OtlpGrpcExporter.to("localhost", 4317)) {
    exporter.traces().consume(traces).toCompletableFuture().join();
    exporter.metrics().consume(metrics).toCompletableFuture().join();
}
```

The concrete gRPC exporter currently uses plaintext credentials. `ClientTransportConfig` defines client endpoint, timeout, TLS, headers, compression, and retry shape; `ServerTransportConfig` defines bind host, port, and TLS shape. The shipped transport only implements the plaintext path today.

### Tap

`TelemetryTap` is for live observation, not pipeline routing:

```java
receiver.tap().setOptions(new TapOptions(BackpressureStrategy.DROP_OLDEST, 256));
receiver.tap().all().subscribe(subscriber);
```

Tap subscriber lag is isolated from the pipeline acknowledgement path unless `BackpressureStrategy.BLOCK` is selected.

## Architectural non-goals

- Do not expose generated proto or gRPC types from `otlp4j-api`.
- Do not rebuild the OpenTelemetry Java SDK.
- Do not add storage, search, aggregation, or retention semantics to the SDK core.
- Do not hide multi-consumer routing behind implicit defaults; make fan-out explicit.
- Do not merge acknowledgement counts across different signals.

## Validation standard

Architecture-sensitive changes should keep these checks true:

- The sample compiles against `otlp4j-api` and reaches the transport only at runtime.
- Public examples use `OtlpGrpcReceiver`, `Pipeline.from(...)`, per-signal consumers, `ConsumeResult<T>`, and `OtlpGrpcExporter` facets.
- No public API signature includes generated OpenTelemetry proto or gRPC types.
- Fan-out tests prove peer failures do not prevent other peers from seeing the batch.
- Batching tests prove traces, metrics, logs, and profiles cannot be cross-mixed.
- Tap tests prove slow subscribers do not affect pipeline acknowledgements under non-blocking strategies.
