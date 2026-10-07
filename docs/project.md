---
title: "otlp4j"
description: "An embeddable Java telemetry pipeline and OTLP gateway library"
tags: ["Java", "OpenTelemetry", "OTLP"]
---

# Project scope

otlp4j gives Java applications and tools control over telemetry that they receive or submit. Its purpose is to make filtering, transformation, routing, buffering, and export composable in Java while keeping generated protocol classes out of application code.

## When it fits

Use otlp4j when telemetry processing benefits from sharing the JVM's code, configuration, and lifecycle:

- Apply application-specific enrichment or redaction before exporting data.
- Receive OTLP in a Java service and route batches to selected destinations.
- Connect trace processing to a metrics pipeline.
- Build test receivers and capturing exporters without deploying a collector.
- Adapt OpenTelemetry Java SDK exporter callbacks to local processing.

Embedding also shares heap, CPU, and failure boundaries with the host application. If standard SDK export already meets the need, another local processing runtime may add little value. If telemetry infrastructure needs independent scaling, upgrades, or durable buffering, evaluate a separate collector. The [comparison guide](comparison.md) explains these choices.

## Composition choices

| Need | Entry point |
| --- | --- |
| One typed signal flow with application submissions | [`Pipeline<T>`](pipelines.md) |
| Several sources or pipelines, named components, or connectors | [`Gateway.builder()`](gateways.md) |
| An owned wire client or receiver without a graph | [`OtlpExporter` or `OtlpReceiver`](transport.md#owned-transports) |
| Instrumentation feeding an in-process input | [`SdkExporters`](sdk-integration.md) |

The library processes telemetry. Use the OpenTelemetry Java API and SDK to create spans, aggregate application metrics, and manage instrumentation context.

## Status and compatibility

The build version is `0.1.0-SNAPSHOT` and requires JDK 25. Public APIs may change between snapshots; no compatibility layer is maintained between them. Install the snapshot from source using [Getting started](getting-started.md).

The repository includes unit, property, integration, wire-conformance, and lifecycle tests. Their presence does not establish production suitability or a throughput guarantee. Evaluate the workloads and failure conditions your application must handle; see [Development](development.md).

## Supported scope and limits

| Area | Supported scope | Boundary |
| --- | --- | --- |
| Signals | Traces, metrics, logs, and profiles | Profiles use `v1development` and preserve opaque bodies and dictionaries; see [Domain model](model.md#profiles) |
| Wire delivery | OTLP/gRPC and OTLP/HTTP binary protobuf | OTLP/JSON and other ingestion protocols are not implemented |
| Processing | Java transforms, built-in processors, and connector contracts | Built-in batch merging excludes profiles; see [Components](components.md#batching) |
| Configuration | Java builders and immutable configuration records | No bundled YAML loader or configuration-reload service |
| Buffering | Bounded in-memory batching and delivery queues | No persistent queue or replay after process termination |
| Security | TLS, client certificates, and outgoing headers | No bundled application authentication or authorization subsystem; see [Transport](transport.md#tls-and-client-authentication) |
| Operational visibility | Optional `GatewayMetrics` SPI | No built-in storage, query engine, or telemetry UI |
| Extensibility | Receiver, processor, exporter, and connector factories | No extension host or plugin-discovery service |

Wire receivers require an explicit downstream route for each signal the application intends to accept. An unwired signal is refused retryably.

For configuration defaults and their practical meaning, use the [component limits](components.md#batching-defaults) and [transport limits](transport.md#receiver-admission-and-size-limits). For delivery guarantees, use [Delivery and lifecycle](delivery.md).
