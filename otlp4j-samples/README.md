# otlp4j samples

This module contains the runnable end-to-end demo for otlp4j. It is intentionally small: it proves the current public API standard, SPI wiring, transport, transforms, connectors, and typed model work together without letting generated proto or gRPC types leak into sample code.

## Demo topology

`OtlpE2eDemo` wires this flow:

```mermaid
flowchart LR
    client["Client OtlpGrpcExporter (5 spans)"] --> gateway["Gateway OtlpGrpcReceiver"]
    gateway --> source["receiver.traces() Source"]
    source --> enrich["Transforms.setTraceResourceAttribute"]
    enrich --> filter["Transforms.keepSpansWhere SERVER"]
    filter --> fanout{"Pipeline fan-out"}
    fanout --> exporter["backendExporter.traces()"]
    exporter --> backend["Backend OtlpGrpcReceiver"]
    fanout --> count["SpanCountConnector"]
    count --> metrics["backendExporter.metrics()"]
    metrics --> backend
```

The client sends five spans. Three `SERVER` spans survive the filter, the backend receives those three spans with the enriched resource attribute, and `SpanCountConnector` emits an `otlp4j.connector.span.count` metric with value `3`.

## Run the sample check

From the repository root:

```sh
./mvnw -B -pl otlp4j-samples -am test -Dtest=OtlpE2eDemoTest -Dsurefire.failIfNoSpecifiedTests=false
```

The test executes the demo with ephemeral ports, so it does not need a local collector or a fixed port. It asserts the expected outcome directly: three spans reach the backend, the derived span-count metric is `3`, and the surviving trace resource has `deployment.environment=demo`.

## What the sample demonstrates

- `otlp4j-samples` compiles against `otlp4j-api` only.
- `otlp4j-transport` is present only at runtime and is discovered through the SPI.
- Telemetry crosses two real plaintext OTLP/gRPC hops.
- `OtlpGrpcReceiver` exposes typed sources such as `receiver.traces()`.
- `Pipeline.from(source)` transforms, filters, and fans out trace batches.
- `OtlpGrpcExporter` exposes typed consumer facets such as `traces()` and `metrics()`.
- A connector can derive metrics from traces.
- The sample code never imports generated proto or gRPC classes.

## Optional package profiles

The module also defines packaging profiles:

```sh
./mvnw -B -pl otlp4j-samples -am package -Pnative
./mvnw -B -pl otlp4j-samples -am package -Pjlink
```

`native` requires a GraalVM JDK on `JAVA_HOME`. `jlink` builds a linked runtime image for the pure API/sample side while keeping the runtime transport stack outside the linked closure.
