# otlp4j, the OpenTelemetry Java SDK, and the Collector

These projects occupy different points on the telemetry path:

```text
application instrumentation -> OpenTelemetry Java SDK -> OTLP -> data plane -> backend
                                                        |
                                      +-----------------+------------------+
                                      |                                    |
                                  otlp4j                             OpenTelemetry Collector
                              embedded JVM library                    standalone process
```

The SDK creates telemetry. otlp4j and the Collector process telemetry that already exists as OTLP. They are complementary rather than interchangeable.

| Concern | OpenTelemetry Java SDK | otlp4j | OpenTelemetry Collector |
| --- | --- | --- | --- |
| Form factor | Library in the instrumented JVM | Library embedded in a JVM | Standalone process |
| Primary input | Tracer, meter, and logger APIs | Typed domain batches or OTLP gRPC/HTTP | OTLP and many contrib protocols |
| Primary output | OTLP and SDK exporters | OTLP gRPC and HTTP | OTLP plus many contrib exporters |
| Assembly | SDK providers and processors | `Pipeline` or `Gateway.builder()` | YAML and a built distribution |
| Queue durability | SDK process memory | In-memory only | Optional persistent sending queue |
| Data model | SDK instrumentation data | Immutable proto-free OTLP records | Mutable `pdata` wrappers |
| Runtime boundary | Application lifecycle | Application lifecycle | Independent deployment lifecycle |

## Use the SDK to create telemetry

The official SDK owns span lifecycle, context propagation, sampling, metric aggregation, resource detection, and SDK processor queues. It provides `Tracer`, `Meter`, and `Logger` APIs that application code should use for instrumentation.

The optional `otlp4j-opentelemetry` module is the explicit integration boundary for adapting official SDK exporter callbacks to in-process otlp4j inputs. Applications that already have OTLP domain batches do not need the SDK bridge.

## Use otlp4j to process OTLP in a JVM

otlp4j is shaped for embedded gateways and tools that need a local OTLP data plane:

- `Pipeline<T>` gives one typed signal flow with `filter`, `map`, configured `via`, and `to` stages.
- `Gateway.builder()` gives named receivers, processors, exporters, connectors, and multiple signal pipelines with typed handles.
- `otlp4j-transport` receives and exports binary OTLP over gRPC or HTTP using Armeria.
- Built-in components provide batching, memory limiting, filtering, routing, and trace-to-metric span counting.
- Immutable model records and `DeliveryResult` make transformations and failure outcomes explicit.
- `GatewayMetrics` provides optional in-process self-observability without forcing a logging backend.

The built-in batch processor supports traces, metrics, and logs; profiles remain one-to-one because their opaque dictionaries cannot be merged safely. Its defaults are intentionally bounded: a `FAIL` queue of 1024 entries, 1024 accepted submissions, 64 MiB of exact serialized bytes, and 1024 metadata partitions.

Use an embedded pipeline when sharing application lifecycle, heap, configuration, and deployment with the data plane is useful. The trade-off is that application and telemetry processing share failure and resource boundaries; a separate Collector provides stronger isolation.

## Use the Collector for an independent data plane

The Collector is the better fit when the deployment needs its broad component ecosystem, YAML configuration, independent scaling, persistent queues, extensions, authentication, or non-OTLP protocols. It runs as a separate process and can be upgraded or restarted independently of the instrumented application.

otlp4j deliberately does not reproduce those operational features. It currently has no persistent queue, YAML loader, extension host, authentication subsystem, or JSON OTLP transport. Profiles use the experimental `v1development` schema and remain opaque for lossless forwarding.

## Typical combinations

| Situation | Suggested shape |
| --- | --- |
| Instrument a service and export directly | OpenTelemetry Java SDK -> OTLP exporter -> backend or Collector |
| Instrument a service and transform locally | SDK -> OTLP exporter -> embedded otlp4j receiver -> backend |
| Produce OTLP batches inside a Java tool | `Pipeline.builder(signal)` -> `OtlpExporterFactory` |
| Build a multi-signal in-process gateway | `Gateway.builder()` -> receivers/processors/connectors/exporters |
| Operate shared telemetry infrastructure | SDKs -> standalone OpenTelemetry Collector -> backends |
