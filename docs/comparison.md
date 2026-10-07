# Choosing the SDK, otlp4j, and a Collector

Use the OpenTelemetry Java SDK to produce application telemetry. Use otlp4j when that telemetry needs custom processing inside a JVM. Use a standalone OpenTelemetry Collector when processing should have its own deployment and resource budget.

## Compare responsibilities

| Concern | OpenTelemetry Java SDK | otlp4j | OpenTelemetry Collector |
| --- | --- | --- | --- |
| Main responsibility | Record, sample, aggregate, and export application telemetry | Receive, transform, route, buffer, and export telemetry | Receive, process, and export telemetry across services |
| Runtime | Library in the instrumented application | Library in a host JVM | Independently deployed process |
| Typical input | Instrumentation APIs | Domain batches, SDK adapter callbacks, or OTLP | OTLP and other protocols available in the chosen distribution |
| Assembly | Providers, SDK processors, and exporters | Java pipeline or gateway builders | Collector configuration and available components |
| Resource boundary | Application | Host JVM | Collector process |
| Durable buffering | SDK queues use process memory | Queues use process memory | Persistent sending queues can be configured with storage support |

The external capabilities in this table are described in the official [Java SDK](https://opentelemetry.io/docs/languages/java/sdk/), [Collector](https://opentelemetry.io/docs/collector/), and [Collector resiliency](https://opentelemetry.io/docs/collector/resiliency/) guides. Collector features depend on its distribution and configuration.

## Choose an embedded pipeline when local logic matters

An otlp4j pipeline can call Java code that enriches resource attributes, masks sensitive fields, or selects telemetry using application-specific rules. This is useful when processing should be configured, tested, and deployed with the host application.

Embedding shares the application's heap and lifecycle. An overloaded processor or unavailable destination consumes the same application's processing budget. Configure [buffering limits](components.md) and [transport budgets](transport.md) for that boundary.

## Choose a separate Collector when isolation matters

A Collector can scale, restart, and upgrade independently of the application. Its component ecosystem also supports inputs, outputs, extensions, and storage options beyond otlp4j's [scope](project.md#supported-scope-and-limits).

If an SDK exporter and a Collector already satisfy your processing requirements, adding otlp4j creates another runtime to configure and operate. Local custom logic is the reason to add it.

## Combine the tools

| Situation | Integration path |
| --- | --- |
| Instrument a service and export without custom local processing | SDK -> SDK exporter -> backend or Collector |
| Transform SDK telemetry locally without a network hop | SDK -> [`SdkExporters`](sdk-integration.md) -> otlp4j input -> OTLP destination |
| Process telemetry received from another producer | OTLP producer -> [`Gateway`](gateways.md) receiver -> processors -> destination |
| Produce telemetry batches in a Java tool | Domain batches -> [`Pipeline`](pipelines.md) -> terminal exporter |
| Apply local policy and central processing | SDK -> otlp4j -> Collector -> backend |

Use the direct SDK bridge when instrumentation and the pipeline share a JVM. A network receiver is useful when producers need an OTLP endpoint, including producers outside that JVM.
