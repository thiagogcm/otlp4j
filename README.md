# otlp4j

otlp4j is a JPMS-modular Java library for receiving, processing, and exporting [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) telemetry.
It is best used as an OTLP gateway/pipeline inside JVM applications and services, with immutable Java records and typed asynchronous APIs; generated Protobuf and gRPC types remain inside the transport modules.

The project is currently experimental `0.1.0-SNAPSHOT` and requires JDK 25. Its built-in transports carry OTLP over **gRPC** and **HTTP** (binary protobuf, `application/x-protobuf`) for traces, metrics, logs, and experimental profiles, over plaintext or TLS, with authentication headers, gzip compression, and retries. The HTTP transport uses only the JDK (`java.net.http` client, `jdk.httpserver` server) — no extra dependencies.

> [!WARNING] This project was developed with the assistance of AI agents and has not undergone thorough testing. Please report any issues you encounter.

## Positioning vs OpenTelemetry Java SDK

Use otlp4j for OTLP data-plane work: receiving, processing, observing, routing, and forwarding telemetry batches.

Use the official OpenTelemetry Java API/SDK for application instrumentation: tracer/meter/logger APIs, span lifecycle and `SpanContext`, context propagation, resource detectors, and metric instruments.

The `Span`, `Metric`, and `LogRecord` builders in otlp4j are OTLP model-construction helpers for batches moving through a gateway/pipeline. They are not an application instrumentation SDK. A common setup is to instrument code with OpenTelemetry Java, then route that emitted OTLP through an embedded otlp4j gateway when you need local processing or forwarding control.

## Capabilities

- Immutable domain models for all four OTLP signals
- Per-signal receivers, sinks, and exporter facets
- Typed pipelines with transforms, filters, observers, and concurrent fan-out
- Queue-backed batching with bounded buffers and configurable overflow policy
- Trace-to-metric and log-to-metric count sinks
- Independent `Flow.Publisher` streams for live telemetry observation
- Opt-in `OTEL_EXPORTER_OTLP_*` exporter configuration via `fromEnvironment()` (builder or static factory)
- Separate gRPC and HTTP transport modules, selected by the exporter/receiver class you instantiate, behind a small `OtlpClient`/`OtlpServer` SPI

## Use from Maven

The snapshot coordinates are intended for a local reactor build. Install them first:

```sh
./mvnw -B install
```

Compile against the public API and add the transport you want — `otlp4j-transport-grpc` for OTLP/gRPC, `otlp4j-transport-http` for OTLP/HTTP, or both. Each transport depends on `otlp4j-api`, and your code instantiates its `Otlp{Grpc,Http}Exporter`/`Otlp{Grpc,Http}Receiver` directly, so the transport is a compile dependency rather than runtime-only:

```xml
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-api</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-transport-grpc</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Entry points

Four types cover almost everything: `OtlpGrpcReceiver` (ingest), `OtlpGrpcExporter` (send), `Pipeline` (the transform/route DSL), and the ready-made building blocks `Transforms` and `BatchingProcessor`. For OTLP/HTTP, swap in `OtlpHttpReceiver` and `OtlpHttpExporter` — same builders and pipeline wiring, default port 4318 instead of 4317. The [Public API](docs/public-api.md) guide maps every type to its package; coming from OpenTelemetry Go, start with its [concept map](docs/public-api.md#if-you-know-opentelemetry-go).

## Hello, telemetry

Receive on an ephemeral port and print each batch:

```java
var receiver = OtlpGrpcReceiver.builder()
        .ephemeralPort()
        .onTraces(traces -> {
            System.out.println("spans=" + traces.spans().size());
            return ConsumeResult.acceptedStage();
        })
        .build()
        .start();
```

## Example

This gateway keeps server spans, enriches their resource, and exports them to another OTLP endpoint:

```java
var receiver = OtlpGrpcReceiver.on("0.0.0.0", 4317).start();

var exporter = OtlpGrpcExporter.to("collector.example.com", 4317);

var subscription = Pipeline.from(receiver.traces())
        .transform(Transforms.keepSpansWhere(
                span -> span.kind() == Span.Kind.SERVER))
        .transform(Transforms.withTracesResourceAttribute(
                "deployment.environment", AttributeValue.of("production")))
        .filter(traces -> !traces.spans().isEmpty())
        .to(exporter.traces());
```

The receiver accepts one consumer per signal source. Use `branch().fanOut(...).join()` when several consumers need the same batch.

> [!IMPORTANT] An exporter facet such as `exporter.traces()` carries the exporter's lifecycle: terminating the pipeline in it (or fanning out to it) hands the exporter's ownership to the subscription, which drains it on shutdown within the shared deadline and flushes it on `forceFlush`. You then shut down just two lifecycles, in order — subscription, then receiver. The explicit `.to(exporter.traces(), exporter)` overload (equivalently, `.owns(exporter)` before the terminal) remains for an exporter the pipeline can't otherwise reach; combining it with a facet is harmless because the drain is idempotent.

```java
subscription.shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
receiver.shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
```

## Modules

| Module | Role |
| --- | --- |
| `otlp4j-model` | JDK-only OTLP domain records |
| `otlp4j-api` | Public core, pipelines, processors, count sinks, exporters/receivers, configuration, and transport SPI |
| `otlp4j-codec` | Internal model⇄proto marshalling shared by the transports |
| `otlp4j-proto` | Generated OTLP messages and gRPC services, qualified-exported to the codec and transports |
| `otlp4j-transport-grpc` | OTLP/gRPC exporter and receiver (`OtlpGrpcExporter`/`OtlpGrpcReceiver`), on gRPC + Netty |
| `otlp4j-transport-http` | OTLP/HTTP exporter and receiver (`OtlpHttpExporter`/`OtlpHttpReceiver`), JDK-only |
| `otlp4j-samples` | Executable end-to-end example |
| `otlp4j-testing` | Shared reactor test fixtures |
| `otlp4j-coverage` | Aggregate JaCoCo report |

See [Public API](docs/public-api.md) for usage and [Architecture](docs/architecture.md) for module boundaries, request flow, and extension points. The [sample README](otlp4j-samples/README.md) describes the executable scenario.

## Build

The Maven wrapper uses Maven 3.9.16; the build accepts Maven 3.9.9 or newer.

```sh
./mvnw -B verify
```

This runs the tests, coverage checks, Protobuf generation, and Javadoc lint. If [`just`](https://just.systems/) is installed, `just ci` also checks the `justfile` format before running the same verification.

## Current limits

- The bundled transports apply the full configuration surface — TLS, headers, compression, and retry on the client; TLS, `bindHost`, and the receiver-hardening limits on the server. A non-wildcard `bindHost` (e.g. `127.0.0.1`) now binds that specific interface; a wildcard host binds every interface. Compression is asymmetric: the client requests gzip, and the server transparently decodes it (gRPC's default decoder, or `Content-Encoding: gzip` over HTTP) with no server-side switch.
- OTLP/HTTP carries binary protobuf (`application/x-protobuf`) only — no JSON — and POSTs each signal to its standard path (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`). The scheme follows TLS (`http` when disabled, otherwise `https`); an endpoint **path prefix** (e.g. `https://host/otlp` → `/otlp/v1/...`) is applied, from the `OTEL_EXPORTER_OTLP_ENDPOINT` URL or the HTTP exporter's `path(...)`. `maxConcurrentCallsPerConnection` is a no-op for HTTP; bound concurrency with a server executor instead.
- Profiles track OpenTelemetry `v1development`. `ProfilesData.Profile` exposes top-level metadata for inspection but forwards losslessly via opaque passthrough: each profile carries its serialized proto bytes and the batch carries the serialized `ProfilesDictionary`, so samples, locations, mappings, string tables, and the original payload re-emit byte-for-byte. Only the resource/scope wrapper is modeled (standard attributes); the profile payload itself is not introspectable.
- An unattached receiver source acknowledges a batch as accepted. Attach every signal that must be processed.
