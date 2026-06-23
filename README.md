# otlp4j

otlp4j is a JPMS-modular Java SDK for receiving, processing, and exporting [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) telemetry. Application code uses immutable Java records and typed asynchronous APIs; generated Protobuf and gRPC types remain inside the transport modules.

The project is currently experimental `0.1.0-SNAPSHOT` and requires JDK 25. Its built-in transport carries OTLP/gRPC for traces, metrics, logs, and experimental profiles, over plaintext or TLS, with authentication headers, gzip compression, and gRPC-native retries.

> [!WARNING]
> This project was developed with the assistance of AI agents and has not undergone thorough testing. Please report any issues you encounter.

## Capabilities

- Immutable domain models for all four OTLP signals
- Per-signal receivers, consumers, and exporter facets
- Typed pipelines with transforms, filters, observers, and concurrent fan-out
- Queue-backed batching with bounded buffers and configurable overflow policy
- Trace-to-metric and log-to-metric count connectors
- Independent `Flow.Publisher` streams for live telemetry observation
- Opt-in `OTEL_EXPORTER_OTLP_*` exporter configuration via `fromEnvironment()`
- A transport SPI with a gRPC implementation loaded through `ServiceLoader`

## Use from Maven

The snapshot coordinates are intended for a local reactor build. Install them first:

```sh
./mvnw -B install
```

Compile against the public API and add the built-in transport at runtime:

```xml
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-api</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>dev.nthings.otlp4j</groupId>
  <artifactId>otlp4j-transport</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <scope>runtime</scope>
</dependency>
```

## Entry points

Four types cover almost everything: `OtlpGrpcReceiver` (ingest), `OtlpGrpcExporter` (send), `Pipeline` (the transform/route DSL), and the ready-made building blocks `Transforms` and `BatchingProcessor`. The [Public API](docs/public-api.md) guide maps every type to its package; coming from OpenTelemetry Go, start with its [concept map](docs/public-api.md#if-you-know-opentelemetry-go).

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
        .to(exporter.traces(), exporter);
```

The receiver accepts one consumer per signal source. Use `branch().fanOut(...).join()` when several consumers need the same batch.

> [!IMPORTANT]
> An exporter facet such as `exporter.traces()` is a consumer view and does **not** transfer ownership of the exporter to the pipeline — so the plain `.to(exporter.traces())` terminal leaks the exporter's lifecycle. Hand ownership to the subscription with the two-arg `.to(exporter.traces(), exporter)` overload above (equivalently, `.owns(exporter)` before the terminal); the subscription then drains it on shutdown within the shared deadline and flushes it on `forceFlush`. With ownership transferred you shut down just two lifecycles, in order — subscription, then receiver. If you keep the one-arg terminal, you must close the exporter yourself between the two.

```java
subscription.shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
receiver.shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
```

## Modules

| Module | Role |
| --- | --- |
| `otlp4j-model` | JDK-only OTLP domain records |
| `otlp4j-api` | Public receivers, pipelines, processors, connectors, exporters, and transport SPI |
| `otlp4j-proto` | Generated OTLP messages and gRPC services, qualified-exported to the transport |
| `otlp4j-transport` | Internal gRPC client/server and wire mappers |
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

- The bundled transport applies the full configuration surface — TLS, headers, compression, and retry on the client; TLS, `bindHost`, and the receiver-hardening limits on the server. A non-wildcard `bindHost` (e.g. `127.0.0.1`) now binds that specific interface; a wildcard host binds every interface. Compression is asymmetric: the client requests gzip, and the server transparently decodes it via gRPC's default decoder with no server-side switch.
- Profiles track OpenTelemetry `v1development`. `ProfilesData.Profile` exposes top-level metadata for inspection but forwards losslessly via opaque passthrough: each profile carries its serialized proto bytes and the batch carries the serialized `ProfilesDictionary`, so samples, locations, mappings, string tables, and the original payload re-emit byte-for-byte. Only the resource/scope wrapper is modeled (standard attributes); the profile payload itself is not introspectable.
- An unattached receiver source acknowledges a batch as accepted. Attach every signal that must be processed.
