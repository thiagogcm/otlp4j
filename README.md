# otlp4j

otlp4j is a JPMS-modular Java SDK for receiving, processing, and exporting [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) telemetry. Application code uses immutable Java records and typed asynchronous APIs; generated Protobuf and gRPC types remain inside the transport modules.

The project is currently experimental `0.1.0-SNAPSHOT` and requires JDK 25. Its built-in transport supports plaintext OTLP/gRPC for traces, metrics, logs, and experimental profiles.

> [!WARNING]
> This project was developed with the assistance of AI agents and has not undergone thorough testing. Please report any issues you encounter.

## Capabilities

- Immutable domain models for all four OTLP signals
- Per-signal receivers, consumers, and exporter facets
- Typed pipelines with transforms, filters, observers, and concurrent fan-out
- Queue-backed batching with bounded buffers and configurable overflow policy
- Trace-to-metric and log-to-metric count connectors
- Independent `Flow.Publisher` streams for live telemetry observation
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

## Example

This receiver keeps server spans, enriches their resource, and exports them to another OTLP endpoint:

```java
var receiver = OtlpGrpcReceiver.builder()
        .endpoint("0.0.0.0", 4317)
        .build()
        .start();

var exporter = OtlpGrpcExporter.to("collector.example.com", 4317);

var subscription = Pipeline.from(receiver.traces())
        .transform(Transforms.keepSpansWhere(
                span -> span.kind() == Span.Kind.SERVER))
        .transform(Transforms.setTraceResourceAttribute(
                "deployment.environment", AttributeValue.of("production")))
        .filter(traces -> !traces.spans().isEmpty())
        .to(exporter.traces());
```

The receiver accepts one consumer per signal source. Use `branch().fanOut(...).join()` when several consumers need the same batch.

Own the three lifecycles explicitly: close the subscription first, then the exporter and receiver. An exporter facet such as `exporter.traces()` is a consumer view and does not transfer ownership of the exporter to the pipeline.

```java
subscription.shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
exporter.close();
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

- The bundled transport uses plaintext gRPC. Its client applies host, port, and deadline; its server applies the port but currently ignores `bindHost`. TLS, headers, compression, and retry settings exist for SPI compatibility but are not implemented by this provider.
- Profiles track OpenTelemetry `v1development`. `ProfilesData.Profile` retains top-level metadata but not sample, location, mapping, or dictionary tables.
- Metric exemplars are not represented in the domain model.
- An unattached receiver source acknowledges a batch as accepted. Attach every signal that must be processed.
