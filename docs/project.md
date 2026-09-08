---
title: "otlp4j"
description: "Java library for receiving, processing, observing, and exporting OpenTelemetry Protocol telemetry"
tags: ["Java", "OpenTelemetry", "OTLP"]
---

## What it does

otlp4j is a Java library for applications and tools that need to work directly with [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) data. It can receive, process, observe, route, and export traces, metrics, logs, and profiles through a typed Java API.

It is suited to embedded telemetry gateways, test tools, specialized processors, and other Java software that needs more control over telemetry flows than simply sending data to a collector. Built-in capabilities include filtering and enrichment, batching, concurrent routing, live observation, and trace-to-metric span counting.

## Project status

otlp4j is experimental and currently released only as a `0.1.0-SNAPSHOT`, and it requires JDK 25. The API and behavior may change, and the project has not yet received the level of testing expected for production use. Breaking changes are expected while the API is consolidated; no compatibility layer is maintained between snapshots. Evaluate it carefully before using it for critical telemetry paths.

## Current limitations

- The consolidated OTLP transport carries all four signals over gRPC and HTTP (binary protobuf) on Armeria, with plaintext or TLS (including mutual TLS, and `bindHost` interface selection on the receiver), authentication headers, gzip compression, and retries that honor a server `Retry-After` / `RetryInfo`.
- Transport defaults cap receiver decoded requests and exporter requests at 64 MiB, and exporter responses at 4 MiB; the limits are configurable in the transport records.
- Profiles track the experimental OpenTelemetry `v1development` schema; payloads round-trip losslessly via opaque passthrough, but the signal is not yet stable and the profile body is not introspectable.
- The built-in batch processor supports traces, metrics, and logs, but not profiles because distinct opaque profile dictionaries cannot be merged losslessly.
- The default batch processor uses a 1024-entry `FAIL` queue and bounds accepted work at 1024 submissions, 64 MiB of exact serialized bytes, and 1024 metadata partitions.
- A pipeline is assembled and configured in Java (`Pipeline.builder(signal)` for a single typed flow, `Gateway.builder()` or a directly-built `GatewayConfig` for a graph): there is no YAML or other declarative configuration format, no authentication or extension subsystem, and queues are in-memory only (no persistent or on-disk sending queue).
- Telemetry reaching a receiver with no configured downstream is rejected retryably, so every required signal must be wired into a pipeline explicitly.

The project aims to provide a focused, Java-native foundation for custom OTLP workflows while keeping protocol-specific types out of application code. Feedback and issue reports are welcome, especially around interoperability and real-world telemetry workloads.
