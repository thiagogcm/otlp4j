# otlp4j

![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/thiagogcm/otlp4j?utm_source=oss&utm_medium=github&utm_campaign=thiagogcm%2Fotlp4j&labelColor=171717&color=FF570A&link=https%3A%2F%2Fcoderabbit.ai&label=CodeRabbit+Reviews)

otlp4j is a JPMS-modular Java library for receiving, processing, and exporting [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) telemetry.
It is best used as an OTLP gateway/pipeline inside JVM applications and services, with immutable Java records and typed asynchronous APIs; generated Protobuf and gRPC types remain inside the transport modules.

The project is currently experimental `0.1.0-SNAPSHOT` and requires JDK 25. Its built-in transport carries OTLP over **gRPC** and **HTTP** (binary protobuf, `application/x-protobuf`) for traces, metrics, logs, and experimental profiles, over plaintext or TLS, with authentication headers, gzip compression, and retries. The gRPC and HTTP wires are served by one consolidated transport module (`otlp4j-transport-otlp`, on Armeria).

> [!WARNING] This project was developed with the assistance of AI agents and has not undergone thorough testing. Please report any issues you encounter.

## Positioning vs OpenTelemetry Java SDK

Use otlp4j for OTLP data-plane work: receiving, processing, observing, routing, and forwarding telemetry batches.

Use the official OpenTelemetry Java API/SDK for application instrumentation: tracer/meter/logger APIs, span lifecycle and `SpanContext`, context propagation, resource detectors, and metric instruments.

The `Span`, `Metric`, and `LogRecord` builders in otlp4j are OTLP model-construction helpers for batches moving through a gateway/pipeline. They are not an application instrumentation SDK. A common setup is to instrument code with OpenTelemetry Java, then route that emitted OTLP through an embedded otlp4j gateway when you need local processing or forwarding control.

## Capabilities

- Immutable domain models for all four OTLP signals
- Per-signal receivers, sinks, and exporter facets
- Typed pipelines with transforms, filters, observers, and concurrent fan-out
- Queue-backed batching with bounded buffers and one shared `OverflowPolicy` (also used by the telemetry tap)
- Signal-conversion connectors: built-in trace-to-metric and log-to-metric count sinks, plus a `(Signal, Signal)`-keyed factory (`Connectors.of(...)`) for your own, each fanning to one or more downstreams with real per-peer parallelism
- Independent `Flow.Publisher` streams for live telemetry observation
- Opt-in `OTEL_EXPORTER_OTLP_*` exporter configuration via `fromEnvironment()` (builder or static factory), including fail-fast `OTEL_EXPORTER_OTLP_PROTOCOL` validation
- Self-observability via a pluggable `GatewayMetrics` SPI: counters and gauges for batcher drops, admission rejections, tap drops, and export outcomes, with a no-op default and a logging sample adapter
- One consolidated OTLP transport module carrying both gRPC and HTTP, behind a small `OtlpClient`/`OtlpServer` SPI, both symmetric `Lifecycle` resources

## Use from Maven

The snapshot coordinates are intended for a local reactor build. Install them first:

```sh
./mvnw -B install
```

Compile against the public core and add the consolidated OTLP transport, which carries both OTLP/gRPC and OTLP/HTTP; your code instantiates its `OtlpArmeria{Grpc,Http}{Receiver,Exporter}Factory` (or the `OtlpArmeriaReceiver`/`OtlpArmeriaExporter` builders) directly, so the transport is a compile dependency rather than runtime-only:

```xml
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-transport-otlp</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Entry points

Four types cover almost everything: `OtlpGrpcReceiver` (ingest), `OtlpGrpcExporter` (send), `Pipeline` (the transform/route DSL), and the ready-made building blocks `Transforms` and `Batcher`. For OTLP/HTTP, swap in `OtlpHttpReceiver` and `OtlpHttpExporter` (same builders and pipeline wiring, default port 4318 instead of 4317). The [Public API](docs/public-api.md) guide maps every type to its package; coming from OpenTelemetry Go, start with its [concept map](docs/public-api.md#if-you-know-opentelemetry-go).

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
var receiver = OtlpGrpcReceiver.builder().setEndpoint("0.0.0.0", 4317).build().start();

var exporter = OtlpGrpcExporter.to("collector.example.com", 4317);

var subscription = Pipeline.from(receiver.traces())
        .transform(Transforms.keepSpansWhere(
                span -> span.kind() == Span.Kind.SERVER))
        .transform(Transforms.withTracesResourceAttribute(
                "deployment.environment", AttributeValue.of("production")))
        .filter(traces -> !traces.spans().isEmpty())
        .to(exporter.traces());
```

The receiver accepts one consumer per signal source. Terminate at `FanOut.of(...)` (`.to(FanOut.of(a, b))`) when several consumers need the same batch.

> [!IMPORTANT] Exporter facets carry the exporter's lifecycle, so attaching one to a pipeline drains the exporter on shutdown, no registration needed. Use `Stage.owns(resource)` only for a resource hidden behind a lambda or a connector's downstream. `Wiring` codifies the shutdown ordering: receivers stop first (in-flight requests finish through the live pipeline), then subscriptions drain, then directly-owned resources close.

```java
Wiring.builder()
        .receiver(receiver)
        .pipeline(subscription)
        .build()
        .shutdown(Duration.ofSeconds(10))
        .toCompletableFuture()
        .join();
```

## Modules

| Module                  | Role                                                                                                   |
| ----------------------- | ------------------------------------------------------------------------------------------------------ |
| `otlp4j-model`          | JDK-only OTLP domain records                                                                           |
| `otlp4j-core`           | Public core, pipelines, transforms, batching, connectors, component contracts, configuration, self-observability (`GatewayMetrics`) |
| `otlp4j-codec`          | Internal model⇄proto marshalling, qualified-exported to the transport                                  |
| `otlp4j-proto`          | Generated OTLP messages and gRPC services, qualified-exported to the codec and transport               |
| `otlp4j-transport-common` | Shared transport layer: `ClientExporter`/`ServerReceiver` wrap an `OtlpClient`/`OtlpServer` into the user-facing exporter/receiver, plus admission control, the neutral transport configs, and the entry-point builder bases |
| `otlp4j-service`        | Component graph and gateway runtime that wires receivers, pipelines, and exporters                     |
| `otlp4j-transport-otlp` | Consolidated OTLP/gRPC and OTLP/HTTP exporter and receiver (`OtlpArmeriaExporter`/`OtlpArmeriaReceiver`), on Armeria |
| `otlp4j-samples`        | Executable end-to-end example                                                                          |
| `otlp4j-testing`        | Shared reactor test fixtures                                                                           |
| `otlp4j-coverage`       | Aggregate JaCoCo report                                                                                |

See [Public API](docs/public-api.md) for usage and [Architecture](docs/architecture.md) for module boundaries, request flow, and extension points. The [sample README](otlp4j-samples/README.md) describes the executable scenario.

## Build

The Maven wrapper uses Maven 3.9.16; the build accepts Maven 3.9.9 or newer.

```sh
./mvnw -B verify
```

This runs the tests, coverage checks, Protobuf generation, and Javadoc lint. If [`just`](https://just.systems/) is installed, `just ci` also checks the `justfile` format before running the same verification.

## Current limits

- The bundled transports apply the full configuration surface: TLS, headers, compression, and retry on the client; TLS, `bindHost`, and the receiver-hardening limits on the server. A non-wildcard `bindHost` (e.g. `127.0.0.1`) now binds that specific interface; a wildcard host binds every interface. Compression is asymmetric: the client requests gzip, and the server transparently decodes it (gRPC's default decoder, or `Content-Encoding: gzip` over HTTP) with no server-side switch.
- OTLP/HTTP carries binary protobuf (`application/x-protobuf`) only (no JSON) and POSTs each signal to its standard path (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`). This is an explicit scope decision, not an oversight: the HTTP receiver rejects any other `Content-Type` with `415 Unsupported Media Type` (an absent header is tolerated), the correct HTTP content-negotiation response rather than an OTLP-spec requirement. The scheme follows TLS (`http` when disabled, otherwise `https`); an endpoint **path prefix** (e.g. `https://host/otlp` → `/otlp/v1/...`) is applied, from the `OTEL_EXPORTER_OTLP_ENDPOINT` URL or the HTTP exporter's `setPath(...)`. `maxConcurrentCallsPerConnection` is a gRPC-only knob (not a shared config field); bound HTTP concurrency with a server executor instead.
- Profiles track OpenTelemetry `v1development`. `ProfilesData.Profile` exposes top-level metadata for inspection but forwards losslessly via opaque passthrough: each profile carries its serialized proto bytes and the batch carries the serialized `ProfilesDictionary`, so samples, locations, mappings, string tables, and the original payload re-emit byte-for-byte. Only the resource/scope wrapper is modeled (standard attributes); the profile payload itself is not introspectable.
- An unattached receiver source rejects retryably. Attach every signal that must be processed, or call `discard()` on signals that should be accepted and dropped intentionally.
