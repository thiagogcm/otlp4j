# Built-in components

Add `otlp4j-components` for reusable processors and the span-count connector. Register a factory with its default configuration or pass an explicit configuration record through a [pipeline](pipelines.md) or [gateway](gateways.md).

| Component | Factory | Configuration | Signals |
| --- | --- | --- | --- |
| Batching | `BatchProcessorFactory` | `BatchProcessorConfig` | Traces, metrics, logs |
| Memory limiting | `MemoryLimiterProcessorFactory` | `MemoryLimiterConfig` | All four |
| Filtering | `FilterProcessorFactory` | `FilterConfig` | All four |
| Routing branch | `RoutingProcessorFactory` | `RoutingConfig` | All four |
| Span counting | `SpanCountConnectorFactory` | `SpanCountConnectorConfig` | Traces -> metrics |

## Batching

The batch processor coalesces accepted submissions, splits output to configured item and serialized-byte caps, and passes it to a bounded delivery queue. Buffering transfers ownership, so inspect [flush reports](delivery.md#flush-reports) for failures after acceptance.

`BatchPolicy.sendBatchSize` is a flush trigger, not a hard output-size cap. Set `sendBatchMaxSize` or `sendBatchMaxBytes` to enforce output limits. A zero hard cap disables that dimension. A zero timeout disables timed flushing; size triggers and explicit flush still apply.

Byte caps measure the complete serialized OTLP request, including resource and scope grouping. They do not measure JVM heap usage. If one indivisible item exceeds an output byte cap, the processor refuses the submission permanently rather than emitting an oversized item.

`metadataKeys` partitions batches by the selected client-header values from `ConsumeContext`. Unset keys, empty value lists, and different header lists form distinct partitions. This partitions submissions by request metadata; it does not split items by resource attributes.

### Batching defaults

| Setting | Default | Meaning |
| --- | --- | --- |
| `batch.sendBatchSize` | 8192 items | Flush trigger |
| `batch.timeout` | 1 second | Timed flush for a partial batch |
| `batch.sendBatchMaxSize` | `0` | No hard item cap |
| `batch.sendBatchMaxBytes` | `0` | No hard output-byte cap |
| `batch.metadataKeys` | Empty | One metadata partition |
| `delivery.queueSize` | 1024 entries | Buffered output queue capacity |
| `delivery.numConsumers` | 1 | Delivery worker count |
| `delivery.overflow` | `FAIL` | Refuse new queued work retryably when full |
| `degradedWindow` | 5 seconds | Temporarily refuse new delivery-queue admissions after downstream rejection |
| `maxPendingSubmissions` | 1024 | Accepted submissions retained across buffering, queueing, and delivery |
| `maxPendingBytes` | 64 MiB | Serialized weight of accepted submissions still retained |
| `maxPartitions` | 1024 | Active metadata buffers awaiting flush |

`maxPendingSubmissions` and `maxPendingBytes` cover accepted work across buffering, queueing, and delivery. `maxPartitions` counts only active batch buffers; a partition stops counting when its buffer moves to the delivery queue. Configure both admission and output limits because queue capacity alone does not bound all retained work.

The degraded window applies at the delivery queue. A submission can receive `Accepted` from the batch processor before that queue refuses its output. Inspect the [flush report](delivery.md#flush-reports) for these later refusals.

### Configure a batch processor

For an existing typed `builder`, this configuration uses a smaller trigger and caps each output request:

```java
import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import java.time.Duration;
import java.util.List;

var config = new BatchProcessorConfig(
        new BatchPolicy(512, 1024, 4L * 1024 * 1024,
                Duration.ofMillis(200), List.of("x-tenant-id")),
        new DeliveryQueuePolicy(128, 1, OverflowPolicy.FAIL),
        Duration.ofSeconds(5),
        256,
        16L * 1024 * 1024,
        64);
builder.via(new BatchProcessorFactory(), config);
```

Profiles are excluded from this processor. Their opaque dictionaries need ownership-preserving handling; see [Profiles](model.md#profiles).

## Memory limiting

The memory limiter samples JVM used heap. Below the hard threshold it forwards payloads; at the hard threshold it refuses ingress retryably with a retry-after hint. It resumes admission when a later sample falls below that threshold. The soft state provides an internal collection hook, but the built-in factory leaves it as a no-op and does not call `System.gc()`.

| Setting                                         | Default  |
| ----------------------------------------------- | -------- |
| Check interval                                  | 1 second |
| Hard threshold (`limitBytes`)                   | 512 MiB  |
| Spike band (`spikeLimitBytes`)                  | 128 MiB  |
| Soft threshold (`limitBytes - spikeLimitBytes`) | 384 MiB  |
| Refusal retry-after hint                        | 1 second |

Choose thresholds for the host application's heap, not just telemetry payload size. Put the limiter before buffering when you want it to refuse incoming work before the batch processor accepts ownership. A sampled limiter is not a reservation system or a hard upper bound on process memory.

## Filtering and routing

These processors keep or discard entire payloads without changing their contents:

| Configuration | Forwarding rule | Default |
| --- | --- | --- |
| `FilterConfig(include, exclude)` | Included and not excluded | Keep everything |
| `RoutingConfig(match)` | Match the branch condition | Forward nothing |

Use `MatchSpec.all()` for an explicit catch-all and `MatchSpec.none()` for no match. A `MatchSpec` matches when any contained rule matches. Within a `MatchRule`, every configured condition must match. Regular expressions match the whole string.

```java
import dev.nthings.otlp4j.components.routing.MatchRule;
import dev.nthings.otlp4j.components.routing.MatchSpec;
import dev.nthings.otlp4j.components.routing.RoutingConfig;

var checkout = MatchRule.builder()
        .resourceAttribute("service.name", "checkout")
        .header("x-tenant-id", "tenant-a")
        .build();
var branch = new RoutingConfig(MatchSpec.of(checkout));
```

Resource checks use the first string value found for a key across the payload's resource groups. Header checks match when at least one value for that key satisfies the condition. These checks use routing metadata without requesting the full domain batch. `domainPredicate(...)` explicitly requests the domain view.

A mixed-resource batch is still forwarded or discarded as one unit. To keep only some of its resource groups or items, add a `map` stage that uses the batch's [resource and item filters](model.md#select-and-replace-items). A routing processor has one downstream branch; build several pipelines sharing a source to route to several destinations. Overlapping matches send the payload to each matching branch.

An intentional filter or routing discard returns `Accepted`. It is a policy decision, not a queue-overflow loss, and is not added to a flush report's dropped-item count.

## Span counting

The connector emits one monotonic delta-sum metric per incoming trace batch. The value is the payload's span count, the unit is `{item}`, and the default name is `otlp4j.connector.span.count`. `SpanCountConnectorConfig` changes the name and description.

The metric uses an empty resource and the connector's instrumentation scope. It does not preserve per-service or per-span dimensions, compute latency histograms, or provide deduplication. Place filtering before the connector when the metric should count only retained spans.

Its delivery result comes from the destination metrics pipeline, so rejection there surfaces back to the trace path. See [connector wiring](gateways.md#derive-another-signal).
