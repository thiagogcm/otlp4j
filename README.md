# otlp4j

![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/thiagogcm/otlp4j?utm_source=oss&utm_medium=github&utm_campaign=thiagogcm%2Fotlp4j&labelColor=171717&color=FF570A&link=https%3A%2F%2Fcoderabbit.ai&label=CodeRabbit+Reviews)

otlp4j is a JPMS-modular Java library for receiving, processing, and exporting [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) telemetry. It is an in-process OTLP gateway and pipeline: you assemble a graph of receivers, processors, exporters, and connectors, and a runtime starts them in dependency order and moves telemetry batches through them. The component model deliberately mirrors the OpenTelemetry Collector's, so a receiver, processor, exporter, or connector means here what it means there, expressed as typed Java with immutable records and no protobuf on the public surface.

The project is currently experimental `0.1.0-SNAPSHOT` and requires JDK 25. Its built-in transport carries OTLP over **gRPC** and **HTTP** (binary protobuf, `application/x-protobuf`) for traces, metrics, logs, and experimental profiles, over plaintext or TLS, with authentication headers, gzip compression, and retries. Both wires are served by one consolidated transport module (`otlp4j-transport`, on Armeria).

> [!WARNING]
> This project was developed with the assistance of AI agents and has not undergone thorough testing. Please report any issues you encounter.

## Positioning vs OpenTelemetry Java SDK

Use otlp4j for OTLP data-plane work: receiving, processing, observing, routing, and forwarding telemetry batches that already exist as OTLP. Use the official OpenTelemetry Java API/SDK for application instrumentation: tracer/meter/logger APIs, span lifecycle, context propagation, resource detectors, and metric instruments. The `Span`, `Metric`, and `LogRecord` builders in otlp4j are OTLP model-construction helpers for batches moving through a gateway, not an application instrumentation SDK. A common setup instruments code with OpenTelemetry Java, then routes that emitted OTLP through an embedded otlp4j gateway for local processing or forwarding control. See [comparison.md](docs/comparison.md) for how otlp4j relates to the SDK and the Collector.

## Capabilities

- Immutable, proto-free domain records for all four OTLP signals (traces, metrics, logs, experimental profiles), with the generated protobuf and gRPC types confined to the transport module.
- The four Collector-shaped component kinds (receiver, processor, exporter, connector) behind typed factories in `otlp4j-core`, assembled into a validated service graph that starts in reverse-topological order and shuts down in topological order with rollback on a failed start.
- One consolidated OTLP transport carrying both gRPC (default port 4317) and HTTP (4318) on Armeria, with plaintext or TLS (including mutual TLS), gzip compression, per-attempt and total timeouts, and retry with server `Retry-After` / `RetryInfo` honoring. Two graph factories, `OtlpReceiverFactory` and `OtlpExporterFactory`, each protocol-parameterized through `grpc()` / `http()`.
- Built-in components in `otlp4j-components`: a batch processor, a memory-limiter processor, filter and routing processors driven by a match engine, and a trace-to-metric span-count connector.
- Delivery results (`Accepted` / `Partial` / `Rejected`, with a retryable-versus-permanent disposition) mapped to OTLP partial-success and gRPC/HTTP status on the wire, plus receiver admission control (in-flight request and byte caps).
- Self-observability through the pluggable `GatewayMetrics` SPI: counters and gauges for export outcomes, admission rejections, tap drops, and connector counts, no-op by default, with a logging adapter shipped in the samples module.
- Ergonomic seams that keep trivial components short: `ComponentConfig.NONE` for a component that needs no config, `ProcessorFactory.of(...)` for an inline batch transform, `ExporterFactory.of(...)` for an inline terminal sink, and `GatewayRuntime.boundPort(id)` for reading a receiver's bound port.

## Use from Maven

The snapshot coordinates are intended for a local reactor build. Install them first:

```sh
./mvnw -B install
```

Compile against the core contracts, the transport that carries both OTLP/gRPC and OTLP/HTTP, and the built-in components. Your code names the transport factories and the built-ins directly, so all three are compile dependencies:

```xml
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-transport</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-components</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Hello, gateway

A gateway is built from `Gateway.builder()`. Each registration returns a typed handle; pipelines wire handles, not strings, so referencing an unregistered or wrong-kind component is a compile error. This one receives OTLP/gRPC on an ephemeral port, drops empty trace batches, and forwards the rest to another OTLP endpoint:

```java
var gw = Gateway.builder();

var in = gw.receiver(ComponentId.parse("in"), OtlpReceiverFactory.grpc(),
        OtlpReceiverConfig.defaults(4317).withEphemeralPort());

var dropEmpty = gw.processor(ComponentId.parse("drop-empty"),
        ProcessorFactory.of("drop-empty", TRACES,
                traces -> traces.spanCount() == 0 ? null : traces, Payloads.DOMAIN));

var out = gw.exporter(ComponentId.parse("out"), OtlpExporterFactory.grpc(),
        OtlpExporterConfig.builder(4317).setHost("collector.example.com").build());

gw.pipeline(PipelineId.of(TRACES), p -> p.from(in).via(dropEmpty).to(out));

GatewayRuntime gateway = gw.build();
gateway.start();
int port = gateway.boundPort(in.id()).orElseThrow(); // the ephemeral receiver port
// ... telemetry flows ...
gateway.close(); // topological shutdown within the grace period
```

`TRACES` is `SignalDescriptor.TRACES`; `Payloads.DOMAIN` (from `otlp4j-codec`) is the emit seam a domain-transform processor writes through. A processor that carries configuration takes a factory and a config record instead of the inline `ProcessorFactory.of` form, exactly as the built-in `BatchProcessorFactory`, `FilterProcessorFactory`, `RoutingProcessorFactory`, `MemoryLimiterProcessorFactory`, and `SpanCountConnectorFactory` do. The [samples module](otlp4j-samples/src/main/java/dev/nthings/otlp4j/samples/OtlpE2eDemo.java) shows a full two-hop demo wiring enrich, redact, filter, batch, and a span-count connector.

For a standalone exporter or receiver outside a graph, `OtlpExporter.grpcBuilder()` / `OtlpExporter.httpBuilder()` and `OtlpReceiver.grpcBuilder()` / `OtlpReceiver.httpBuilder()` build the same transport directly.

## Modules

| Module              | Role                                                                                                                        |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `otlp4j-model`      | JDK-only immutable OTLP domain records for the four signals                                                                 |
| `otlp4j-proto`      | Generated OTLP messages and gRPC stubs, qualified-exported only to the codec and transport                                  |
| `otlp4j-core`       | Component SPI (four kinds plus factories), signal descriptors, the opaque `TelemetryPayload` and its views, delivery results, neutral policies, and the `GatewayMetrics` SPI |
| `otlp4j-codec`      | Model-to-proto marshalling behind the proto-free `Payloads` facade                                                          |
| `otlp4j-service`    | Declarative `GatewayConfig`, the JGraphT service graph, validation diagnostics, and the `Gateway` builder and `GatewayRuntime` |
| `otlp4j-transport`  | The consolidated OTLP/gRPC and OTLP/HTTP transport on Armeria: two graph factories and two standalone entry points          |
| `otlp4j-components` | Built-in batch, memory-limiter, filter, and routing processors, and the span-count connector                               |
| `otlp4j-samples`    | Runnable end-to-end demo and the `LoggingGatewayMetrics` sample adapter                                                     |
| `otlp4j-testing`    | Shared reactor fixtures, the reusable conformance and policy suites, and `RecordingGatewayMetrics`                          |
| `otlp4j-conformance`| Armeria instantiations of the conformance and policy suites                                                                |
| `otlp4j-coverage`   | Aggregate JaCoCo report                                                                                                     |

The `otlp4j-benchmarks` JMH module builds only under the `-Pbench` profile, so the default reactor is eleven modules. See [Public API](docs/public-api.md) for the exported-package map and [Architecture](docs/architecture.md) for the component model, payload design, graph lifecycle, and transport internals.

## Build

The Maven wrapper uses Maven 3.9.16; the build accepts Maven 3.9.9 or newer.

```sh
./mvnw -B verify
```

This runs the tests, coverage checks, Protobuf generation, and Javadoc lint. If [`just`](https://just.systems/) is installed, `just ci` also checks the `justfile` format before running the same verification.

## Current limits

- One consolidated transport applies the full configuration surface: TLS, headers, compression, and retry on the client; TLS, `bindHost`, and the receiver-hardening admission limits on the server. A non-wildcard `bindHost` (for example `127.0.0.1`) binds that specific interface; a wildcard host binds every interface. Compression is asymmetric: the client requests gzip and the server transparently decodes it, with no server-side switch.
- OTLP/HTTP carries binary protobuf (`application/x-protobuf`) only (no JSON) and POSTs each signal to its standard path (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`). The HTTP receiver rejects any other `Content-Type` with `415 Unsupported Media Type` (an absent header is tolerated). The scheme follows TLS (`http` when disabled, otherwise `https`); an endpoint path prefix is applied.
- Profiles track OpenTelemetry `v1development`. `ProfilesData` exposes top-level resource and scope metadata for inspection but forwards the profile payload losslessly via opaque passthrough, so it re-emits byte-for-byte; the profile body itself is not introspectable.
- Queues are in-memory only: there is no persistent or on-disk sending queue, no YAML configuration format, and no authentication or extension subsystem. A pipeline is assembled and configured in Java.
- A signal reaching a receiver with no downstream is rejected retryably. Wire every signal that must be processed into a pipeline.
