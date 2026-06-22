---
title: "otlp4j"
description: "Java SDK for receiving, processing, observing, and exporting OpenTelemetry Protocol telemetry"
tags: [ "Java", "OpenTelemetry", "OTLP" ]
---

## What it does

otlp4j is a Java SDK for applications and tools that need to work directly with [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) data. It can receive, process, observe, route, and export traces, metrics, logs, and profiles through a typed Java API.

It is suited to embedded telemetry gateways, test tools, specialized processors, and other Java software that needs more control over telemetry flows than simply sending data to a collector. Built-in capabilities include filtering and enrichment, batching, concurrent routing, live observation, and basic trace-to-metric and log-to-metric conversion.

## Project status

otlp4j is experimental and currently released only as a `0.1.0-SNAPSHOT`. The API and behavior may change, and the project has not yet received the level of testing expected for production use. Evaluate it carefully before using it for critical telemetry paths.

## Current limitations

- The built-in OTLP transport supports plaintext and TLS gRPC (with `bindHost` interface selection), authentication headers, gzip compression, and gRPC-native automatic retries.
- Profiles track the experimental OpenTelemetry `v1development` schema; payloads round-trip losslessly via opaque passthrough, but the signal is not yet stable.
- Telemetry received without a configured consumer is acknowledged but not processed, so every required signal must be connected explicitly.

The project aims to provide a focused, Java-native foundation for custom OTLP workflows while keeping protocol-specific types out of application code. Feedback and issue reports are welcome, especially around interoperability and real-world telemetry workloads.
