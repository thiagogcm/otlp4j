---
title: "otlp4j"
description: "Java library for receiving, processing, observing, and exporting OpenTelemetry Protocol telemetry"
tags: ["Java", "OpenTelemetry", "OTLP"]
---

## What it does

otlp4j is a Java library for applications and tools that need to work directly with [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) data. It can receive, process, observe, route, and export traces, metrics, logs, and profiles through a typed Java API.

It is suited to embedded telemetry gateways, test tools, specialized processors, and other Java software that needs more control over telemetry flows than simply sending data to a collector. Built-in capabilities include filtering and enrichment, batching, concurrent routing, live observation, and basic trace-to-metric and log-to-metric conversion.

## Project status

otlp4j is experimental and currently released only as a `0.1.0-SNAPSHOT`, and it requires JDK 25. The API and behavior may change, and the project has not yet received the level of testing expected for production use. Evaluate it carefully before using it for critical telemetry paths.

## Current limitations

- The consolidated OTLP transport carries all four signals over gRPC and HTTP (binary protobuf) on Armeria, with plaintext or TLS (including mutual TLS, and `bindHost` interface selection on the receiver), authentication headers, gzip compression, and retries that honor a server `Retry-After` / `RetryInfo`.
- Profiles track the experimental OpenTelemetry `v1development` schema; payloads round-trip losslessly via opaque passthrough, but the signal is not yet stable and the profile body is not introspectable.
- A gateway is assembled and configured in Java (`Gateway.builder()` or a directly-built `GatewayConfig`): there is no YAML or other declarative configuration format, no authentication or extension subsystem, and queues are in-memory only (no persistent or on-disk sending queue).
- Telemetry reaching a receiver with no configured downstream is rejected retryably, so every required signal must be wired into a pipeline explicitly.

The project aims to provide a focused, Java-native foundation for custom OTLP workflows while keeping protocol-specific types out of application code. Feedback and issue reports are welcome, especially around interoperability and real-world telemetry workloads.
