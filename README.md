# otlp4j

otlp4j is a small, JPMS-modular Java 25 library for receiving, processing, and exporting [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) telemetry in-process. It is useful when an application, test, or tool needs an OTLP data plane without running a separate collector process.

The primary API is a typed pipeline over immutable domain batches. Applications submit traces, metrics, logs, or profiles; processors compose transformations and filters; an OTLP exporter sends the result over gRPC or HTTP. The pipeline owns a mutable component runtime during its lifecycle. The lower-level gateway API is available when a graph needs multiple receivers, fan-out, connectors, or independently named components.

The project is experimental (`0.1.0-SNAPSHOT`) and intentionally makes breaking changes while the public API is consolidated. It requires JDK 25 and has no compatibility layer for earlier snapshots.

## A first pipeline

This pipeline filters and transforms immutable trace batches before exporting them. `start()` owns transport startup, `submit()` reports direct admission or rejection according to the configured component policy, and `flush()` reports asynchronous downstream losses after buffered acceptance.

`flush(timeout)` captures in-progress deliveries at each application input, then snapshots downstream buffers as they are reached. Submissions concurrent with a downstream snapshot may be included; exact global exclusion requires barrier propagation through every buffering component.

```java
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Pipeline;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import java.time.Duration;

try (var traces = Pipeline.builder(TRACES)
        .filter(batch -> batch.spanCount() > 0)
        .map(MyTransforms::redactSensitiveFields)
        .to(OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(4317))
        .build()) {
    traces.start();
    var result = traces.submit(batch).toCompletableFuture().join();
    var report = traces.flush(Duration.ofSeconds(5)).toCompletableFuture().join();
}
```

`Pipeline.Builder` also accepts configured processors through `via(factory, config)`, and multiple terminal exporters through repeated `to(...)` calls. `Pipeline` is generic over its signal, so a trace pipeline cannot accidentally receive a metrics batch.

## Maven dependencies

Install the snapshot locally while the project is experimental:

```sh
./mvnw -B install
```

For the first pipeline, depend directly on the model, service, and transport modules:

```xml
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-model</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-service</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-transport</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Add `otlp4j-components` for the built-in processors and connectors. Add `otlp4j-opentelemetry` only when adapting official OpenTelemetry Java SDK exporters.

## Maven modules

The core runtime is split so protocol implementation details do not leak into application code:

| Module | Purpose |
| --- | --- |
| `otlp4j-model` | Immutable, proto-free OTLP domain records for traces, metrics, logs, and profiles |
| `otlp4j-core` | Signal descriptors, payloads, component contracts, delivery results, policies, and telemetry SPI |
| `otlp4j-service` | Typed `Pipeline` API, advanced `Gateway` graph builder, lifecycle, and validation |
| `otlp4j-transport` | Armeria OTLP/gRPC and OTLP/HTTP receiver and exporter factories, TLS, compression, retry, and admission control |
| `otlp4j-codec` | Internal domain/protobuf marshalling used by bundled transport and components |
| `otlp4j-proto` | Generated OTLP protobuf and gRPC classes, qualified-exported only to internal modules |
| `otlp4j-components` | Batch, memory-limiter, filter, routing, and span-count connector components |
| `otlp4j-opentelemetry` | Optional bridge boundary for the official OpenTelemetry Java SDK exporters |
| `otlp4j-samples` | Runnable examples, including an embedded in-process pipeline |
| `otlp4j-testing` | Shared fixtures and protocol/conformance test support |
| `otlp4j-conformance` | Black-box OTLP wire and policy conformance runner |
| `otlp4j-coverage` | Aggregate JaCoCo report |

The JMH benchmark harness is outside the default reactor and is enabled with `-Pbench`.

## Advanced gateway graphs

Use `Gateway.builder()` when the application needs network receivers, multiple pipelines, typed component handles, or signal-to-signal connectors:

```java
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.OtlpExporterFactory;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.OtlpReceiverFactory;

var gateway = Gateway.builder();
var input = gateway.receiver(
        ComponentId.parse("in"),
        OtlpReceiverFactory.grpc(),
        OtlpReceiverConfig.defaults(4317).withEphemeralPort());
var output = gateway.exporter(
        ComponentId.parse("out"),
        OtlpExporterFactory.grpc(),
        OtlpExporterConfig.defaults(4317));

gateway.pipeline(PipelineId.of(TRACES), path -> path.from(input).to(output));
try (var runtime = gateway.build()) {
    runtime.start();
    int port = runtime.boundPort(input.id()).orElseThrow();
    // OTLP traffic can now enter at `port`.
}
```

The handle-based DSL catches wrong component kinds and handles from another builder before runtime construction. `Gateway.compile(GatewayConfig)` remains available for callers that build the declarative configuration records directly. Start and shutdown are derived from the graph and are bounded by `ServiceOptions`.

## SDK bridge

`otlp4j-opentelemetry` is deliberately isolated from the protocol-neutral modules. It is the place for adapters between the official OpenTelemetry Java SDK's trace, metric, and log exporters and otlp4j's in-process inputs. Applications that only submit OTLP domain batches do not need this module. Use the official SDK for instrumentation, context propagation, sampling, and aggregation; use otlp4j for an embedded OTLP data plane.

The adapters borrow their `TelemetryInput`; they snapshot SDK data into the immutable model, and their `shutdown()` drains accepted input work without closing or stopping the input. Close the SDK provider before its borrowed pipeline. SDK exporter shutdown waits for outstanding input submissions and then performs the adapter's bounded input flush, so an input implementation must complete its submission stages:

```java
var traces = Pipeline.builder(SignalDescriptor.TRACES)
        .to(OtlpExporterFactory.grpc(), OtlpExporterConfig.defaults(4317))
        .build();
var sdkExporter = SdkExporters.spans(traces);
var tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(sdkExporter))
        .build();

try (traces; tracerProvider) {
    traces.start();
    // Instrumentation uses tracerProvider; completed spans enter the pipeline.
}
```

Use `SdkExporters.metrics(metricsInput)` for the default cumulative aggregation temporality, or pass an explicit official `AggregationTemporalitySelector` when the SDK reader requires a different policy. `SdkExporters.logs(logsInput)` provides the equivalent log-record adapter.

The bridge targets OpenTelemetry Java SDK 1.65.0 and maps every field exposed by its exporter interfaces. The SDK does not expose OTLP point flags, exponential-histogram zero thresholds, resource or scope dropped-attribute counts, or OTLP metric metadata; the adapter emits the model defaults for those fields (zero flags/counts/threshold and empty metadata). It preserves span and link remote-context bits and snapshots mutable SDK collections and byte values before submission.

## Build

The Maven wrapper uses Maven 3.9.16 and the build requires Maven 3.9.9 or newer:

```sh
./mvnw -B verify
```

If `just` is installed, `just ci` checks the recipe formatting and runs the same verification. Generate the checked-in schema mirror and protobuf sources with:

```sh
just update-protos
just proto
```

The mirrored schemas are pinned to upstream `opentelemetry-proto` v1.11.0 and retain their Apache License 2.0 headers.

## Boundaries and current limits

- OTLP/HTTP uses binary protobuf (`application/x-protobuf`) and the standard signal paths; JSON is not implemented.
- Queues are in-memory only. There is no persistent sending queue, YAML configuration, extension host, or authentication subsystem.
- Profiles use the experimental `v1development` schema. The profile body is preserved opaquely for lossless forwarding rather than exposed as a fully modeled domain type.
- The built-in batch processor does not support profiles, because merging distinct profile dictionaries would require re-indexing. Forward profiles one-to-one or use a component that preserves their dictionary ownership.
- Protobuf and gRPC generated classes stay behind the codec/transport JPMS boundary.
- Delivery results distinguish accepted, partial, and retryable/permanent rejection outcomes. A successful submission to an in-process input means admission, not completion of every downstream export; call `flush()` when a drain report is needed.

The default hardening and buffering limits are deliberately explicit:

| Surface | Default |
| --- | --- |
| Batch trigger | 8192 items or 1 second; no hard item/byte batch cap |
| Batch delivery queue | 1024 entries, one consumer, `OverflowPolicy.FAIL` |
| Batch admission | 1024 accepted submissions, 64 MiB exact serialized bytes, 1024 metadata partitions |
| Receiver request | 64 MiB maximum decoded export request |
| Exporter messages | 64 MiB maximum request and 4 MiB maximum response |

See [Architecture](docs/architecture.md), [Public API](docs/public-api.md), and [Comparison](docs/comparison.md) for the design and integration trade-offs. The [samples](otlp4j-samples/README.md) contain a complete runnable example.
