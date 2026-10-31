# Built-in components

Add `otlp4j-components` for reusable processors and the span-count connector. Register a factory with its default configuration or pass an explicit configuration record through a [pipeline](pipelines.md) or [gateway](gateways.md).

| Component | Factory | Configuration | Signals |
| --- | --- | --- | --- |
| Batching | `BatchProcessorFactory` | `BatchProcessorConfig` | All four |
| Memory limiting | `MemoryLimiterProcessorFactory` | `MemoryLimiterConfig` | All four |
| Filtering and routing | `FilterProcessorFactory` | `FilterConfig` | All four |
| Span counting | `SpanCountConnectorFactory` | `SpanCountConnectorConfig` | Traces -> metrics |

## Batching

The batch processor coalesces accepted submissions, splits output to configured item and byte caps, and passes it to a bounded delivery queue. Buffering transfers ownership, so inspect [flush reports](delivery.md#flush-reports) for failures after acceptance.

`BatchPolicy.sendBatchSize` is a flush trigger, not a hard output-size cap. Set `sendBatchMaxSize` or `sendBatchMaxBytes` to enforce output limits. A zero hard cap disables that dimension. A zero timeout disables timed flushing; size triggers and explicit flush still apply.

`metadataKeys` partitions batches by the selected client-header values from `ConsumeContext`. Unset keys, empty value lists, and different header lists form distinct partitions. This partitions submissions by request metadata; it does not split items by resource attributes.

### How submissions are weighed

Every byte figure comes from the configured `sizer`. A submission the sizer weighs counts toward `maxPendingBytes`. A submission it weighs as zero counts its items toward `maxPendingItems` instead.

| Sizer | Weighs | Cost |
| --- | --- | --- |
| `PayloadSizer.reported()`, the default | Payloads that carry an encoded size: those received over OTLP, including one whose batch a later stage replaced | None for a payload as received; one encode for a replaced one |
| `OtlpPayloadSizer.exact()` from `otlp4j-transport` | Every payload, at the exact size of its OTLP export request including resource and scope grouping | One discarded encode for each payload without a wire form |

Payloads without a wire form include batches submitted in process, SDK bridge exports, connector output, and the batches this processor builds. Under the default sizer they are bounded by `maxPendingSubmissions` and `maxPendingItems`, and weighing them costs nothing.

A hard byte cap (`sendBatchMaxBytes`) has to weigh the batches the processor builds, so the configuration rejects it unless a sizer other than the default is set. With the exact sizer the cap measures the complete serialized request. If one indivisible item exceeds an output byte cap, the processor refuses the submission permanently rather than emitting an oversized item. It also refuses a submission whose split would weigh more than 64 times its size: every request repeats its resource, its scope, and any profile dictionary entries its items share, so when those nearly fill the cap each request holds few items and the requests together would dwarf the submission. Byte limits do not measure JVM heap usage.

A submission that would split into more requests than half of `delivery.queueSize` is refused permanently too, with either cap. The queue admits the requests of a flush as a group, so one submission that could fill it would otherwise take the items buffered beside it down with it. Raise `queueSize` if your largest submissions need more requests. For the same reason, a configuration with a hard cap is rejected unless `queueSize` is at least 64.

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
| `maxPendingBytes` | 64 MiB | Weight, as measured by the sizer, of accepted submissions still retained |
| `maxPartitions` | 1024 | Active metadata buffers awaiting flush |
| `maxPendingItems` | 262,144 items | Items still retained from submissions the sizer weighs as zero |
| `sizer` | `PayloadSizer.reported()` | Weigh only payloads that carry an encoded size |

`maxPendingSubmissions`, `maxPendingBytes`, and `maxPendingItems` cover accepted work across buffering, queueing, and delivery. The default item limit is what the default byte limit holds at 256 bytes per item. It is a starting point and it bounds a count, not memory: when item sizes vary widely, as with large log bodies, configure the exact sizer so the byte limit applies. A submission the sizer cannot weigh is admitted even when it exceeds `maxPendingItems`, provided no other unweighed work is pending; otherwise it is refused retryably, and under continuous unweighed traffic that can keep happening. A weighed submission above `maxPendingBytes` is always refused. `maxPartitions` counts only active batch buffers; a partition stops counting when its buffer moves to the delivery queue. Configure both admission and output limits because queue capacity alone does not bound all retained work.

The degraded window applies at the delivery queue. A submission can receive `Accepted` from the batch processor before that queue refuses its output. Inspect the [flush report](delivery.md#flush-reports) for these later refusals.

### Configure a batch processor

For an existing typed `builder`, this configuration uses a smaller trigger and caps each output request. The byte cap needs the exact sizer, so the example also depends on `otlp4j-transport`:

```java
import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.transport.OtlpPayloadSizer;
import java.time.Duration;
import java.util.List;

var config = new BatchProcessorConfig(
        new BatchPolicy(512, 1024, 4L * 1024 * 1024,
                Duration.ofMillis(200), List.of("x-tenant-id")),
        new DeliveryQueuePolicy(128, 1, OverflowPolicy.FAIL),
        Duration.ofSeconds(5),
        256,
        16L * 1024 * 1024,
        64,
        32_768,
        OtlpPayloadSizer.exact());
builder.via(new BatchProcessorFactory(), config);
```

The last two arguments are `maxPendingItems` and the sizer. Without a byte cap, the six-argument constructor keeps their defaults.

### Batch profiles

The processor counts a profile as one item however many samples it holds, which is the unit of OTLP's `rejected_profiles`. One profile can outweigh thousands of spans, and the item settings assume about 256 bytes per item, so on their own they neither shape batches nor bound memory for this signal:

- `sendBatchSize` rarely fires, so a partial batch normally waits for the timeout, and only a byte cap limits the size of the merged request.
- Under the default sizer, profiles built in process or exported by the SDK bridge never count toward `maxPendingBytes`. Only `maxPendingSubmissions` and `maxPendingItems` limit them, which can retain far more memory than `maxPendingBytes` suggests. Profiles received over OTLP carry their size and are weighed.

Configure every batching profile pipeline with `OtlpPayloadSizer.exact()` and a `sendBatchMaxBytes` below the destination's request limit. Without a cap, encoding a merged batch can pass the exporter's `maxRequestBytes`, and the exporter then refuses that whole batch after its senders were told `Accepted`. Merged batches then split into requests within the cap, each with a [dictionary](model.md#profiles) holding only what it references, and pending profiles count at their encoded size. Weighing a payload without a wire form costs one encode, including its dictionary. A batch above the cap costs more, because the processor encodes each part it builds to weigh it. It splits a batch it flushes while holding the lock that admits new submissions, so a cap near the size of a typical submission keeps that work short.

A profile does not split: dividing one would repeat its id and change the `rejected_profiles` count of a partial success. A submission whose encoded form exceeds `sendBatchMaxBytes` and holds a profile that cannot fit is refused permanently, smaller profiles beside it included, so keep the cap above the largest profile your sources produce. A profile can also weigh more alone than inside its submission, because a dictionary numbers entries by first use, so a merged batch can fail to split although each submission fits the cap. The submissions then go out as separate requests instead of being merged, and a failure among them stays with that submission and appears in the [flush report](delivery.md#flush-reports).

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

The filter processor decides each resource group of a payload separately. It forwards the groups that match `include` and do not match `exclude`, each with all its items, and discards the rest.

| Configuration | Kept resource groups |
| --- | --- |
| `FilterConfig.defaults()` | All |
| `FilterConfig.keep(match)` | Those matching `match` |
| `new FilterConfig(include, exclude)` | Those matching `include` and not `exclude` |

Use `MatchSpec.all()` for an explicit catch-all and `MatchSpec.none()` for no match. A `MatchSpec` matches when any contained rule matches. Within a `MatchRule`, every configured condition must match. Regular expressions match the whole string.

```java
import dev.nthings.otlp4j.components.routing.FilterConfig;
import dev.nthings.otlp4j.components.routing.MatchRule;
import dev.nthings.otlp4j.components.routing.MatchSpec;

var checkout = MatchRule.builder()
        .resourceAttribute("service.name", "checkout")
        .header("x-tenant-id", "tenant-a")
        .build();
var branch = FilterConfig.keep(MatchSpec.of(checkout));
```

Conditions apply at two levels:

| Condition | Tested against | Outcome within one payload |
| --- | --- | --- |
| `resourceAttribute`, `resourceAttributeRegex` | Each resource group's string value for the key | Can differ between resource groups |
| `header`, `headerRegex` | The request's client headers; one matching value is enough | Same for every resource group |
| `domainPredicate` | The whole domain batch | Same for every resource group |

A payload whose groups are all kept is forwarded as received, and a payload whose groups are all discarded is not forwarded. Neither case requests the domain batch unless a rule uses `domainPredicate(...)`. A payload with both kept and discarded groups is decoded and forwarded with only its kept groups, so that reduced batch is encoded again on export. A payload without resource groups cannot satisfy a resource condition; its header and domain conditions still decide it.

To route to several destinations, [build several pipelines](gateways.md#wire-named-components) that share a source and start each with its own filter. Each branch receives exactly the resource groups its condition selects, and overlapping conditions send a group to every matching branch. To select individual items instead of resource groups, add a `map` stage that uses the batch's [item filters](model.md#select-and-replace-items).

A payload the filter discards entirely returns `Accepted`. When some groups are kept, the result is the downstream consumer's result for the forwarded groups. A discard is a policy decision, not a queue-overflow loss, and is not added to a flush report's dropped-item count.

## Span counting

The connector emits one monotonic delta-sum metric per incoming trace batch. The value is the payload's span count, the unit is `{item}`, and the default name is `otlp4j.connector.span.count`. `SpanCountConnectorConfig` changes the name and description.

The metric uses an empty resource and the connector's instrumentation scope. It does not preserve per-service or per-span dimensions, compute latency histograms, or provide deduplication. Place filtering before the connector when the metric should count only retained spans.

Its delivery result comes from the destination metrics pipeline, so rejection there surfaces back to the trace path. See [connector wiring](gateways.md#derive-another-signal).
