# Typed pipelines

Use `Pipeline<T>` when your application submits one telemetry signal directly. The signal descriptor fixes the batch type: a trace pipeline accepts `TracesData`, and a metrics pipeline accepts `MetricsData`.

For dependencies and a complete first program, start with [Getting started](getting-started.md). For network ingress or connected signals, use [Gateway graphs](gateways.md).

## Assemble a pipeline

The builder applies processors in registration order and sends the resulting payload to every registered terminal exporter.

| Builder method | Effect |
| --- | --- |
| `filter(predicate)` | Keep or discard a whole batch |
| `map(transform)` | Replace a batch with the transform's result |
| `via(factory)` | Add a processor with its default configuration |
| `via(factory, config)` | Add a configured processor |
| `to(factory)` or `to(factory, config)` | Add a terminal exporter; repeat for fan-out |
| `service(options)` | Set the graph's lifecycle budgets |

At least one terminal exporter is required. Buffering is explicit: the input, `filter`, and `map` stages add no implicit queue.

This example exports an existing trace `batch` to an OTLP/gRPC destination on localhost. The destination must be running and accept traces.

```java
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.service.Pipeline;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;

try (var traces = Pipeline.builder(TRACES)
        .filter(data -> data.spanCount() > 0)
        .to(OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(4317))
        .build()) {
    traces.start();
    var result = traces.submit(batch).toCompletableFuture().join();
    System.out.println(result);
}
```

Use [Transport](transport.md) to configure a different endpoint, TLS, headers, or retry policy. Handle the result according to [Delivery and lifecycle](delivery.md#submission-results).

## Transform immutable batches

A `map` stage returns a replacement batch. The batch types' [item and resource operations](model.md#select-and-replace-items) keep resource and scope grouping, schema URLs, and the items a transform does not change.

For example, these stages add a resource attribute to every resource group and keep only server spans. Insert them before `to(...)`:

```java
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Span;

// builder is a Pipeline.Builder<TracesData>.
builder.map(data -> data.mapResources(resource -> resource.withAttribute(
                "deployment.environment", AttributeValue.of("production"))))
        .map(data -> data.filterSpans(span -> span.kind() == Span.Kind.SERVER));
```

`filter` keeps or discards an entire batch; select items in a `map` stage. A batch left without items is still forwarded, so follow the selection with `filter(data -> data.spanCount() > 0)` when empty batches should stop there. The [end-to-end sample](../otlp4j-samples/src/main/java/dev/nthings/otlp4j/samples/OtlpE2eDemo.java) combines resource enrichment, span redaction, and span-kind selection.

Transforms run synchronously on the invoking processing path. Use a component with explicit scheduling or buffering for work that needs a separate execution boundary.

## Add buffering deliberately

Add `otlp4j-components` and place a batch processor before the exporter:

```java
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.service.Pipeline;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import java.time.Duration;

try (var traces = Pipeline.builder(TRACES)
        .via(new BatchProcessorFactory())
        .to(OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(4317))
        .build()) {
    traces.start();
    var admission = traces.submit(batch).toCompletableFuture().join();
    System.out.println(admission);
    var report = traces.flush(Duration.ofSeconds(5)).toCompletableFuture().join();
    if (!report.isSuccess()) {
        throw new IllegalStateException("Buffered delivery loss: " + report);
    }
}
```

Inspect both values. The submission reports admission to the buffer; the flush report exposes subsequent delivery losses. Configure the batch processor's [triggers, caps, and overflow behavior](components.md#batching) for your workload.

## Own the runtime

`build()` returns an unstarted pipeline. Call `start()` before submission, then use `flush(timeout)` when you need a drain report and `shutdown(timeout)` or `close()` to release owned components.

A builder freezes its definition after the first build. Further `build()` calls create independent runtimes from that definition. Submissions require a running runtime; a closed pipeline cannot restart.

See [Lifecycle and deadlines](delivery.md#lifecycle-and-deadlines) for startup rollback, shutdown ordering, and timeout behavior.
