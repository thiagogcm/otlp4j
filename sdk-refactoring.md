# otlp4j SDK refactoring proposal

**Status:** proposal, awaiting acceptance. Greenfield, breaking changes welcomed.
**Authors:** reconciled from a Java architect + OTel-spec/Collector expert debate.
**Date:** 2026-05-18.

## TL;DR

1. **Split `TelemetryConsumer` into four per-signal `Consumer<T>` interfaces** mirroring the OpenTelemetry Collector. Stop silently acknowledging unsupported signals.
2. **Make every consumer call asynchronous** — `CompletionStage<ConsumeResult<T>>`. Java 25 virtual threads make this free at the call site and unlock real fan-out.
3. **Replace `ExportResult` with sealed, signal-parameterised `ConsumeResult<T>`** (`Accepted` / `Partial` / `Rejected`). Fix the partial-success fan-out aggregation bug.
4. **Add a `Pipeline` graph builder with first-class fan-out and named `Receiver`/`Processor`/`Connector`/`Exporter` roles.** Promote `OtlpReceiver`→`OtlpGrpcReceiver` and grow a `Receiver` SPI; same on the exporter side.
5. **Expose a streaming side-channel as `TelemetryTap`** backed by `java.util.concurrent.Flow.Publisher<Telemetry>`. No third-party reactive library; coroutine and Reactor users adapt via the standard JDK bridge.
6. **Rebuild `BatchProcessor`** as an asynchronous, timer-triggered, queue-backed `BatchingProcessor` with drop counters, `forceFlush(Duration)`, and `shutdown(Duration)` — matching upstream OTel-SDK semantics it currently only pretends to have.
7. **Grow the transport SPI** with `ClientTransportConfig` / `ServerTransportConfig` records (TLS, headers, compression, retry policy, metadata), keeping proto/gRPC types behind the SPI as today.
8. **Kotlin-first ergonomics throughout**: nullable returns instead of `Optional`; sealed types for exhaustive `when`; single-method SAMs at every plug-in point; last-lambda DSL hooks; unchecked exceptions.

The refactor touches every module except `otlp4j-proto`. The wire SPI boundary holds — generated proto/gRPC types still never leak into application code.

---

## Why now

The current API is small and internally consistent, but it has shape problems that compound the moment the SDK is used as more than a sample:

- **`TelemetryConsumer` has four `consumeX(...)` default methods that all return `ExportResult.success()`** (`otlp4j-api/.../pipeline/TelemetryConsumer.java:13-30`). This is convenient for one-class implementations and a footgun in any larger pipeline. The real-world consequence is visible in the project's own demo: `OtlpE2eDemo.java:83-88` defines a fan-out that overrides only `consumeTraces`; any metrics, logs, or profiles flowing through that stage are *acknowledged as success* and dropped on the floor. The same trap lives in `CountConnector.java:28-39`, which overrides only `consumeTraces` and `consumeLogs`. The compiler cannot help.
- **`ExportResult.and(...)` (`pipeline/ExportResult.java:38-50`) sums rejected counts** across two results and concatenates messages. That's the wrong arithmetic for fan-out: when one batch goes to N downstreams and 3 of them reject 10 records each, the *original sender* still saw at most 10 rejections — not 30. The same operation is also misused inside `BatchProcessor.flush()` (`processor/BatchProcessor.java:94-108`) to combine across *different signals*, where a single `rejectedCount` field is not meaningful at all. OTLP 1.7 carries `rejected_spans`, `rejected_log_records`, `rejected_data_points`, `rejected_profiles` as *separate* fields for exactly this reason.
- **Synchronous everything.** `OtlpClient.exportX` is blocking (`spi/OtlpClient.java:14-20`); `BatchProcessor` flushes on the gRPC server thread (`processor/BatchProcessor.java:57-91` is `synchronized` and forwards inline); the receiver→consumer call runs on whatever thread the transport happened to use. Multi-consumer fan-out, real-time streaming taps, and back-pressure are all unbuildable on this substrate without first paying the async tax.
- **`Pipeline` is a `List<Processor>` with a `.into(terminal)` (`pipeline/Pipeline.java:30-36`).** It does not model branches, named stages, lifecycle, routing, or fan-out — the things a *pipeline* in OTel-Collector parlance actually is. Fan-out becomes the hand-rolled anonymous-class pattern in the demo.
- **Receiver builder duality.** `OtlpReceiver.Builder` accepts *either* `consumer(...)` *or* a bag of `*Handler` lambdas (`receiver/OtlpReceiver.java:111-150`), with `consumer` silently winning if both are set (`OtlpReceiver.java:29-37`). Two ways to spell the same idea, both half-implemented; the per-signal `*Handler` SAMs (`receiver/TraceHandler.java` and friends) exist only to give the builder a lambda-friendly setter.
- **`BatchProcessor` is misnamed for its semantics.** Upstream OpenTelemetry-Java's `BatchSpanProcessor` is asynchronous, queue-backed, timer-triggered, has `forceFlush()` and `shutdown(timeout)`, and counts drops. otlp4j's `BatchProcessor` has none of these — see the explicit "no background flush thread" call-out in `docs/architecture.md:53`. Users reading the name assume OTel-SDK behaviour and get a synchronous buffer.
- **SPI is too thin.** `OtlpClientProvider.create(host, port, timeout)` (`spi/OtlpClientProvider.java:13`) has no room for TLS, headers, compression, retry policy, or per-call metadata. `SpiSupport.findFirst()` (`api/internal/SpiSupport.java:13-19`) bakes in one transport per JVM. Both are correct for v0.1; both must grow before alternate transports (`otlphttp`, kafka, file tail) or production deployments are possible.
- **Kotlin friction.** `Attributes.get` returns `Optional<AttributeValue>` (`model/Attributes.java:37-39`) — Kotlin idiom is nullable. `OtlpReceiver.start(int)` throws checked `IOException` (`OtlpReceiver.java:45`). `TelemetryConsumer` has four methods — Kotlin's SAM conversion cannot apply, so users have to write full `object : TelemetryConsumer { ... }` literals.

---

## Goals

1. Streamline the user-facing API.
2. Support real-time streaming to multiple concurrent consumers, with back-pressure and independent failure isolation.
3. Be a first-class Kotlin SDK as well as a Java SDK — coroutines, sealed `when`, last-lambda DSLs.
4. Preserve OTel-Collector mental model (Receiver → Processor* → Connector? → Exporter, per-signal pipelines).
5. Keep proto/gRPC out of the public surface.

## Non-goals

- Replacing the OTel-Java SDK. This library is the *Collector*-shaped Java SDK; it remains complementary to OpenTelemetry-Java.
- Vendoring Reactor / RxJava / Mutiny.
- Implementing OTLP/Arrow or bidirectional gRPC streaming in v1 (the API shape will be portable when those land).

---

## Target architecture

```
                       ┌────────────────────────┐
                       │   Receiver (SPI)       │     OtlpGrpcReceiver
                       │   - subscribe(Tap)     │     OtlpHttpReceiver (future)
                       └──────────┬─────────────┘     KafkaReceiver (future)
                                  │
                  per-signal Consumer<T> calls
                                  │
                       ┌──────────▼─────────────┐
                       │   Pipeline (graph)     │
                       │   transform / filter   │
                       │   branch / fanOut      │
                       │   batch                │
                       │   connector            │
                       └──────────┬─────────────┘
                                  │
                       ┌──────────▼─────────────┐
                       │   Exporter (SPI)       │     OtlpGrpcExporter
                       │   - send + lifecycle   │     OtlpHttpExporter (future)
                       └────────────────────────┘     ConsoleExporter

       ⤷ TelemetryTap (Flow.Publisher<Telemetry>) — side-channel, not in pipeline path
```

Three observation surfaces, three contracts:

| Surface | Type | Back-pressure | Failure isolation |
|---|---|---|---|
| **Pipeline consumer** | `Consumer<T>.consume(T): CompletionStage<ConsumeResult<T>>` | Async with no demand signalling. Slow consumer → unflushed buffer → partial_success / `RESOURCE_EXHAUSTED` back to sender. | `FanOut<T>` runs children concurrently; one child failing does not block others. |
| **Streaming tap** | `Flow.Publisher<Telemetry>` (sealed envelope) | JDK `Flow.Subscription.request(n)`. Configurable `BackpressureStrategy` if a subscriber lags. | Tap subscribers are **independent of the pipeline**. A tap that errors or cancels never affects pipeline acks. |
| **Per-signal handler sugar** | `Builder.onTraces(lambda)` etc. | None — single subscriber, runs inline on the dispatcher thread. | Direct error → transport-level failure. |

---

## Detailed design

### 1. Per-signal `Consumer<T>` family

Replace the unified `TelemetryConsumer` with four single-method SAMs, parameterised by the OTLP signal type:

```java
package dev.nthings.otlp4j.pipeline;

@FunctionalInterface
public interface Consumer<T> {
    CompletionStage<ConsumeResult<T>> consume(T batch);

    default Capabilities capabilities() { return Capabilities.IMMUTABLE; }

    enum Capabilities { IMMUTABLE, MUTATES_DATA }
}

public interface TraceConsumer    extends Consumer<TraceData>    {}
public interface MetricConsumer   extends Consumer<MetricsData>  {}
public interface LogConsumer      extends Consumer<LogsData>     {}
public interface ProfileConsumer  extends Consumer<ProfilesData> {}
```

This mirrors Go Collector's `consumer.Traces` / `consumer.Metrics` / ... and gives us four properties at once:

- **Compile-time wiring.** A metrics-only exporter cannot be wired to a log terminal. The `CountConnector` silent-drop trap becomes impossible: `CountConnector` declares `implements TraceConsumer, LogConsumer, MetricsProducer` (where the latter is a downstream link, not a `Consumer`), and metrics/profiles never reach it.
- **Honest fan-out.** The aggregation rule is well-defined per signal type (see §3 below).
- **Kotlin SAM conversion.** Each interface is single-method, so `TraceConsumer { traces -> … }` lambdas work directly.
- **Capabilities flag.** Lets fan-out reuse buffers when *every* peer is non-mutating, and defensively copy otherwise. Matches Collector behaviour.

For users who genuinely want "one consumer for everything" (debug printers, file taps), provide a composite:

```java
public final class CompositeConsumer {
    public static Builder builder();
    public static final class Builder {
        public Builder traces(TraceConsumer c);
        public Builder metrics(MetricConsumer c);
        public Builder logs(LogConsumer c);
        public Builder profiles(ProfileConsumer c);
        public Pipeline asPipeline();
    }
}
```

`TelemetryConsumer` is **removed**. `ForwardingConsumer` is removed (with single-method SAMs, there's nothing to forward).

### 2. `ConsumeResult<T>` — sealed, signal-typed, async-friendly

```java
public sealed interface ConsumeResult<T>
        permits ConsumeResult.Accepted, ConsumeResult.Partial, ConsumeResult.Rejected {

    record Accepted<T>() implements ConsumeResult<T> {
        public static <T> Accepted<T> instance() { return new Accepted<>(); }
    }

    record Partial<T>(long rejectedItems, String message) implements ConsumeResult<T> {
        public Partial {
            if (rejectedItems < 1) throw new IllegalArgumentException("rejectedItems must be >= 1");
            if (message == null) message = "";
        }
    }

    record Rejected<T>(String message, Throwable cause) implements ConsumeResult<T> {}

    static <T> ConsumeResult<T> accepted()                              { return Accepted.instance(); }
    static <T> ConsumeResult<T> partial(long rejected, String message)  { return new Partial<>(rejected, message); }
    static <T> ConsumeResult<T> rejected(String message)                { return new Rejected<>(message, null); }
    static <T> ConsumeResult<T> rejected(String message, Throwable t)   { return new Rejected<>(message, t); }
}
```

Why this shape:

- **`<T>` is the signal type.** Cross-signal `.and()` is now impossible at compile time. `BatchProcessor.flush()`'s current summing of `rejected_spans + rejected_log_records` is no longer expressible.
- **Sealed + records** → exhaustive `switch` in Java; exhaustive `when` in Kotlin. No `isFullSuccess()` boolean trap, no "rejected vs partial encoded as count==0 and a message" trick.
- **Two combine operations, not one.** See §3.

### 3. Fan-out aggregation — the bug fix

`ExportResult.and(...)` summed rejected counts. That's wrong for fan-out. Use two distinct operators:

```java
public static <T> ConsumeResult<T> fanOutMerge(List<ConsumeResult<T>> peers) {
    // Same batch sent to N peers in parallel.
    // - Any Rejected dominates (the batch failed for at least one consumer).
    // - Otherwise, take MAX of rejectedItems across peers (the worst-case view from the sender).
    // - Messages concatenated for diagnostics.
}

public static <T> ConsumeResult<T> sequentialMerge(ConsumeResult<T> first, ConsumeResult<T> next) {
    // Same batch passes through stage A then stage B sequentially.
    // - If first is Rejected, return first (batch never reached B).
    // - Otherwise rejection counts SUM (different items rejected at different stages).
}
```

`FanOut<T>` (below) uses `fanOutMerge`. `BatchingProcessor`'s downstream call returns the downstream's `ConsumeResult` *directly* — no cross-signal summing.

### 4. `FanOut<T>` and the streaming model

Fan-out is a first-class pipeline component, mirroring the Collector's `consumer.NewTraces([]consumer.Traces)` helper:

```java
public final class FanOut<T> implements Consumer<T> {

    public static <T> FanOut<T> of(List<? extends Consumer<T>> peers);

    @Override
    public CompletionStage<ConsumeResult<T>> consume(T batch) {
        var copies = prepareCopies(batch, peers);   // defensive only if any peer MUTATES_DATA
        var futures = IntStream.range(0, peers.size())
            .mapToObj(i -> peers.get(i).consume(copies.get(i))
                .exceptionally(t -> ConsumeResult.<T>rejected(t.getMessage(), t)))
            .toList();
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(v -> ConsumeResult.fanOutMerge(collect(futures)));
    }

    @Override public Capabilities capabilities() {
        return peers.stream().anyMatch(p -> p.capabilities() == Capabilities.MUTATES_DATA)
                ? Capabilities.MUTATES_DATA : Capabilities.IMMUTABLE;
    }
}
```

**Streaming-to-multiple-consumers** lives at *two* layers:

- **In-pipeline fan-out** — `FanOut<T>` above. Synchronous unary OTLP in, N async consumers out, one aggregated `ConsumeResult` back. All consumers see every batch. One consumer's failure does not block others. Back-pressure is implicit: a slow consumer's `CompletionStage` doesn't complete; the receiver's gRPC handler holds the unary call open until `Duration.timeout` or `allOf` completes, then returns aggregated `partial_success` or `RESOURCE_EXHAUSTED`.

- **Side-channel tap** — `TelemetryTap` (next section). Subscribe to a *live* publisher; never affects the in-pipeline ack.

### 5. `TelemetryTap` — real-time streaming, side-channel

```java
public interface TelemetryTap {
    Flow.Publisher<TraceData>    traces();
    Flow.Publisher<MetricsData>  metrics();
    Flow.Publisher<LogsData>     logs();
    Flow.Publisher<ProfilesData> profiles();
    Flow.Publisher<Telemetry>    all();   // sealed envelope for "give me the firehose"
}

public sealed interface Telemetry permits Telemetry.Traces, Telemetry.Metrics, Telemetry.Logs, Telemetry.Profiles {
    record Traces(TraceData data)        implements Telemetry {}
    record Metrics(MetricsData data)     implements Telemetry {}
    record Logs(LogsData data)           implements Telemetry {}
    record Profiles(ProfilesData data)   implements Telemetry {}
}

public enum BackpressureStrategy { BLOCK, DROP_OLDEST, DROP_NEWEST, ERROR }

public interface TapOptions {
    static TapOptions of(BackpressureStrategy strategy, int bufferSize);
}
```

**Critical property: the tap is decoupled from the pipeline.** When a receiver decodes a batch, it (a) feeds it into the configured pipeline as today, in parallel (b) offers it to each tap subscriber's buffer. If a tap subscriber lags, its strategy decides what happens to *its* buffer — the pipeline-side ack is never affected. The default is `BackpressureStrategy.DROP_OLDEST` with a `bufferSize` of 256 (chosen to avoid head-of-line blocking; users who care about lossless taps must opt into `BLOCK` and accept that they're now in the critical path).

Why `Flow.Publisher` instead of vendoring Reactor:

- Zero new dependencies. `otlp4j-api` keeps its current `requires`: `dev.nthings.otlp4j.model` and `org.slf4j`. `java.util.concurrent.Flow` is JDK-native.
- Reactive Streams TCK compliance for free.
- Kotlin coroutine bridge is one line (`kotlinx-coroutines-reactive` provides `Publisher<T>.asFlow()` via `FlowAdapters.toPublisher(jdkFlow)`).
- Project Reactor users adapt via `FlowAdapters` likewise.
- Virtual threads (Java 25) make the multicast implementation trivial: one bounded queue + one virtual thread per subscription.

Drop counters are surfaced as metrics on the tap itself, exposed to in-process OTel SDKs without recursion.

### 6. `Pipeline` graph builder

Replace `Pipeline.builder().process(...).into(terminal)` with a graph builder that supports the operations the demo currently has to hand-roll:

```java
public final class Pipeline {

    public static <T> Stage<T> from(Source<T> source);                 // Source<T> = Receiver, Consumer<T>, etc.

    public interface Stage<T> {
        Stage<T>            transform(Transform<T> fn);                 // 1→1
        Stage<T>            filter(Predicate<? super T> p);             // 1→0/1
        Stage<T>            tap(Consumer<T> observer);                  // side-effect, never alters flow
        <U> Stage<U>        connect(Connector<T, U> connector);         // type-changing
        Branch<T>           branch();                                   // 1→N
        Subscription        to(Consumer<T> terminal);
    }

    public interface Branch<T> {
        Branch<T>           fanOut(Consumer<T> leaf);
        Subscription        join();
    }

    public interface Connector<I, O> {
        Capabilities capabilities();
        CompletionStage<ConsumeResult<I>> consume(I in, Consumer<O> out);
    }
}
```

`Subscription` is `AutoCloseable`; `close()` propagates `shutdown(Duration)` to every component in the graph in topological order, aggregating timeouts. This is the lifecycle that `Pipeline.into(...)` returns nothing for today.

Connectors stay first-class — they're allowed to change signal type, unlike processors. `CountConnector` becomes `Connector<TraceData, MetricsData>` and `Connector<LogsData, MetricsData>`.

### 7. Receiver / Exporter SPI shape

```java
public interface Receiver extends AutoCloseable {
    Receiver start();                                                   // host/port are config, not method args
    CompletionStage<Void> shutdown(Duration timeout);
    CompletionStage<Void> forceFlush(Duration timeout);
    TelemetryTap tap();                                                 // every receiver exposes one
}

public interface Exporter<T> extends Consumer<T>, AutoCloseable {
    CompletionStage<Void> shutdown(Duration timeout);
    CompletionStage<Void> forceFlush(Duration timeout);
}
```

`OtlpReceiver` becomes `OtlpGrpcReceiver` (concrete implementation) + `Receiver` (interface). `OtlpGrpcExporter` stays the concrete name, gains alongside `OtlpHttpExporter` (future).

**Receiver builder unification.** Drop the two-modes-coexist mess; one builder, with per-signal handler convenience sugar:

```java
var receiver = OtlpGrpcReceiver.builder()
    .endpoint("0.0.0.0", 4317)
    .transport(ServerTransportConfig.tls(certFile, keyFile))
    .onTraces(    traces  -> CompletableFuture.completedFuture(ConsumeResult.<TraceData>accepted()))
    .onMetrics(   metrics -> ...)
    .build()
    .start();
```

Internally, `.onTraces(...)` registers a `TraceConsumer`; `.pipeline(...)` registers the whole graph. They compose (the pipeline runs first, the handler sugar attaches as an inline terminal if the user used handlers instead of a graph). Builder rejects ambiguous configurations at `build()`-time rather than silently picking a winner.

### 8. Transport SPI growth

```java
public interface OtlpClientProvider {
    OtlpClient create(ClientTransportConfig config);
}

public record ClientTransportConfig(
        String host,
        int port,
        Duration timeout,
        Tls tls,
        Map<String, String> headers,
        Compression compression,
        RetryPolicy retry,
        Set<TransportOption> options) {

    public sealed interface Tls permits Tls.Disabled, Tls.SystemTrust, Tls.Custom {
        record Disabled() implements Tls {}
        record SystemTrust() implements Tls {}
        record Custom(Path certFile, Path keyFile, Path trustFile) implements Tls {}
    }

    public sealed interface TransportOption {}     // open-ended; transports may extend with their own
}

public interface OtlpClient extends AutoCloseable {
    CompletionStage<ConsumeResult<TraceData>>    exportTraces(TraceData traces);
    CompletionStage<ConsumeResult<MetricsData>>  exportMetrics(MetricsData metrics);
    CompletionStage<ConsumeResult<LogsData>>     exportLogs(LogsData logs);
    CompletionStage<ConsumeResult<ProfilesData>> exportProfiles(ProfilesData profiles);
    @Override void close();
}
```

`SpiSupport.findFirst()` becomes `provider(Class, String name)` so multiple transports can coexist (`"grpc"`, `"http"`). The receiver/exporter builders select by name; default is `"grpc"` for backward shape compatibility.

`OtlpClient` becoming async is the right boundary: a synchronous SPI on top of an async API forces every caller to spawn threads to fan out, which is the current footgun.

### 9. `BatchingProcessor` — proper OTel-SDK semantics

```java
public final class BatchingProcessor<T> implements Consumer<T>, AutoCloseable {
    public static <T> Builder<T> builder(Class<T> signal);

    public static final class Builder<T> {
        public Builder<T> downstream(Consumer<T> downstream);
        public Builder<T> maxBatchSize(int items);
        public Builder<T> maxBatchAge(Duration age);
        public Builder<T> queueCapacity(int items);                     // drop policy upstream of the queue
        public Builder<T> scheduler(ScheduledExecutorService scheduler);
        public Builder<T> dropPolicy(DropPolicy policy);                // DROP_OLDEST/DROP_NEWEST/BLOCK/ERROR
        public Builder<T> dropCounter(LongAdder drops);                 // observability hook
        public BatchingProcessor<T> build();
    }

    @Override public CompletionStage<ConsumeResult<T>> consume(T batch);

    public CompletionStage<Void> forceFlush(Duration timeout);
    public CompletionStage<Void> shutdown(Duration timeout);
}
```

Properties (now matching upstream OTel-Java's `BatchSpanProcessor`):

- **Per-signal** — type-parameterised, so the four-buffer single-class design goes away.
- **Asynchronous** — `consume()` returns immediately after enqueueing.
- **Timer-triggered** — flush on size *or* age, whichever fires first.
- **Drop counters** — visible to operators.
- **Lifecycle parity** — `forceFlush(Duration)` and `shutdown(Duration)` return futures that report actual drain success/failure.

### 10. Lifecycle parity

Every `Receiver`, `BatchingProcessor`, `Connector`, `Exporter`, and `Pipeline.Subscription` implements:

```java
CompletionStage<Void> shutdown(Duration timeout);
CompletionStage<Void> forceFlush(Duration timeout);
```

`AutoCloseable.close()` delegates to `shutdown(defaultTimeout)`. This matches `opentelemetry-java`'s `SdkTracerProvider.shutdown()` / `forceFlush()` and lets batch jobs detect tail-end loss.

### 11. Model module touch-ups

Minimal but Kotlin-impactful:

- `Attributes.get(String)` returns `AttributeValue` (nullable, annotated `@Nullable`) instead of `Optional<AttributeValue>` (`model/Attributes.java:37-39`).
- `Span.flags` change to `int` with documented unsigned interpretation (replaces silent truncation called out in `docs/architecture.md:87`).
- `Span`, `LogRecord`, `Resource` gain `withFoo(...)` methods for selective copy (Kotlin can't `copy()` Java records).
- `@Experimental` annotation applied to the entire profiles surface; signal still flows through `ProfileConsumer`.

---

## Before / after — three concrete call sites

### Call site A: fan-out in `OtlpE2eDemo.java:83-88`

**Before** (silent drop on metrics/logs/profiles):
```java
var fanOut = new TelemetryConsumer() {
    @Override
    public ExportResult consumeTraces(TraceData traces) {
        return toBackend.consumeTraces(traces).and(spanCounter.consumeTraces(traces));
    }
};
var gatewayPipeline = Pipeline.builder()
    .process(Processors.setResourceAttribute("deployment.environment", AttributeValue.of("demo")))
    .process(Processors.filterSpans(s -> s.kind() == Span.Kind.SERVER))
    .into(fanOut);
var gateway = OtlpReceiver.builder().consumer(gatewayPipeline).build().start(0);
```

**After** (cannot silently drop a signal — the pipeline is typed):
```java
var subscription = Pipeline.from(receiver.traces())
    .transform(Transforms.setResourceAttribute("deployment.environment", "demo"))
    .filter(span -> span.kind() == Span.Kind.SERVER)
    .branch()
        .fanOut(backendExporter)
        .fanOut(new SpanCountConnector(metricsExporter))
    .join();
```

### Call site B: per-signal handler receiver

**Before**:
```java
var receiver = OtlpReceiver.builder()
    .traceHandler(t   -> { sink.set(t);                  return ExportResult.success(); })
    .metricsHandler(m -> { metricsSink.addAll(m.metrics()); return ExportResult.success(); })
    .build().start(0);
```

**After** (returns `CompletionStage`; `Accepted.instance()` is a free constant):
```java
var receiver = OtlpGrpcReceiver.builder()
    .ephemeralPort()
    .onTraces(t   -> { sink.set(t);                  return completed(ConsumeResult.accepted()); })
    .onMetrics(m  -> { metricsSink.addAll(m.metrics()); return completed(ConsumeResult.accepted()); })
    .build().start();
```

### Call site C: live tap from Kotlin

There's nothing like this today. After:
```kotlin
val receiver = OtlpGrpcReceiver.builder().endpoint("0.0.0.0", 4317).build().start()

scope.launch {
    receiver.tap().traces().asFlow().collect { traces ->
        liveDashboard.push(traces.spans().take(10))
    }
}
```

The collector receives a copy of every batch the receiver accepts. If the dashboard lags, the configured `BackpressureStrategy.DROP_OLDEST` drops on the *tap* buffer; the OTel sender still gets its ack.

---

## Kotlin DX

The decisions above are deliberately Kotlin-aware. Concretely:

1. **Single-method SAMs everywhere a user plugs in** — `TraceConsumer`, `MetricConsumer`, `LogConsumer`, `ProfileConsumer`, `Transform<T>`, `Connector<I, O>`. Kotlin's SAM conversion handles them as lambdas. The previous multi-method `TelemetryConsumer` *could not be SAM-converted*; users had to write `object : TelemetryConsumer { ... }` and pay the silent-drop tax for unimplemented methods.

2. **Sealed types with record permits** — `ConsumeResult`, `Telemetry`, `AttributeValue`. Kotlin reads each `permits` as a sealed-hierarchy member and gives exhaustive `when`:
   ```kotlin
   when (result) {
       is ConsumeResult.Accepted -> log.debug("ok")
       is ConsumeResult.Partial  -> log.warn("${result.rejectedItems} rejected: ${result.message}")
       is ConsumeResult.Rejected -> log.error("dropped: ${result.message}")
   }
   ```

3. **`@Nullable` instead of `Optional`** — `Attributes.get(key): AttributeValue?` becomes Kotlin idiomatic `?:` / `?.let { }`. `Optional` on hot paths is the single biggest Java-Kotlin friction point in upstream OTel-Java.

4. **Last-lambda DSL hooks via `Consumer<Builder>` overloads** (also useful from Java):
   ```java
   public static Span span(Consumer<Span.Builder> spec) {
       var b = Span.builder(); spec.accept(b); return b.build();
   }
   ```
   Reads in Kotlin as:
   ```kotlin
   val span = Span.span {
       name("GET /cart"); kind(Span.Kind.SERVER)
       attributes(attributes {
           string("http.route", "/cart")
           long("http.status_code", 200)
       })
   }
   ```

5. **Unchecked exceptions at boundaries.** `OtlpReceiver.start(int)` (currently `throws IOException`, `receiver/OtlpReceiver.java:45`) becomes unchecked `OtlpReceiverException`. Kotlin doesn't care; Java users no longer need a try/catch for bind failures.

6. **`CompletionStage`, not `CompletableFuture`.** `kotlinx-coroutines` provides `.await()` on `CompletionStage`; returning `CompletableFuture` invites `.cancel(true)` and breaks invariants.

7. **`Flow.Publisher` for the tap surface, not `Flux`/`Multi`.** `kotlinx-coroutines-reactive` covers it with `asFlow()` out of the box.

8. **Avoid Kotlin keywords.** `Pipeline.into(...)` becomes `Pipeline.to(...)` — but **note:** `to` is fine in method position; Kotlin's `to` infix function is only `to` at the `Pair` keyword level. Confirmed no real collisions in the proposed surface (verified against the Kotlin keyword list: `object`, `fun`, `val`, `when`, `in`, `is`, `as`, `typealias`).

9. **`@JvmStatic` is Kotlin-side**, but the Java equivalent is: prefer **static factory methods on the type itself** over builders for short cases. Provide `OtlpGrpcExporter.to("host", 4317)` next to the builder; Kotlin users hate one-call builders.

10. **No `AttributeKey<T>` typed-key pattern from upstream OTel-Java.** otlp4j already has the right shape (`Attributes.builder().put("http.route", "/cart")`, `model/Attributes.java:87-106`) — keep it. Add a Kotlin extension shim in a forthcoming `otlp4j-kotlin` artifact for `attributes { string("k", "v") }` DSL.

---

## Migration impact

| Module | Change | LoC delta (est.) |
|---|---|---|
| `otlp4j-model` | `Optional` → nullable on `Attributes`; add `with*` to a few records; `@Experimental` on profiles; `Span.flags` typing | +60 / −20 |
| `otlp4j-api/pipeline` | **Rewrite.** `TelemetryConsumer`, `Pipeline`, `Processor`, `ForwardingConsumer`, `ExportResult` removed. New: `Consumer<T>` family, `ConsumeResult<T>`, new `Pipeline` graph, `FanOut<T>`, `TelemetryTap`, `Telemetry` envelope, `BackpressureStrategy`. | ~+900 / −350 |
| `otlp4j-api/receiver` | Drop per-signal `*Handler` SAMs. `OtlpReceiver` → `OtlpGrpcReceiver` + `Receiver` interface. Builder unified. | ~+250 / −180 |
| `otlp4j-api/exporter` | `OtlpGrpcExporter` async; `Exporter<T>` typed; lifecycle methods return futures. | ~+150 / −80 |
| `otlp4j-api/processor` | `BatchProcessor` → `BatchingProcessor<T>` rebuilt (timer, queue, drops, lifecycle). `Processors` becomes `Transforms`, scoped by signal. | ~+400 / −190 |
| `otlp4j-api/connector` | `Connector` becomes `Connector<I, O>`; `CountConnector` splits into `SpanCountConnector`, `LogRecordCountConnector`. | ~+100 / −60 |
| `otlp4j-api/spi` | `OtlpClient` async; `ClientTransportConfig` / `ServerTransportConfig` records; named provider selection in `SpiSupport`. | ~+200 / −60 |
| `otlp4j-transport` | Mappers stay; adapt to async SPI and to the new `ServerTransportConfig` (still no TLS implementation in v1 — config carries `Tls.Disabled`); proto/gRPC types stay confined. | ~+250 / −120 |
| `otlp4j-proto` | **No change.** | 0 |
| `otlp4j-samples` | Demo rewritten; shorter, fan-out is no longer hand-rolled. | ~−40 |
| `otlp4j-testing` | Add `TestSubscriber<Telemetry>` recorder + `RecordingConsumer<T>` for per-signal asserts. | ~+200 |
| `otlp4j-kotlin` (new, optional) | Extension functions: attributes DSL, suspend wrappers, `Flow<T>` extensions for tap. | ~+300 (new module) |

**Public type count, net:** ~10 fewer top-level types (handler SAMs, `ForwardingConsumer`, `Processor`, `Pipeline.Builder`, `TelemetryConsumer` removed; `Consumer<T>` family, `TelemetryTap`, `Telemetry`, `BackpressureStrategy` added).

SPI boundary: **preserved**. `otlp4j-proto`'s package exports are unchanged. The async SPI exposes `CompletionStage`, not gRPC's `ListenableFuture`, so the architectural rule that proto/gRPC don't leak still holds.

---

## Phased rollout

The refactor is large, but it can land in five sequential PRs without long-lived feature branches:

1. **PR 1 — `ConsumeResult<T>` and async signatures.** Introduce `ConsumeResult<T>`. Change `TelemetryConsumer.consumeX` to return `CompletionStage<ConsumeResult<X>>`. Adapter from old `ExportResult` for one release cycle, then delete. Updates `OtlpClient`, `OtlpServer`, demo, tests.
2. **PR 2 — Per-signal `Consumer<T>` family.** Add the four interfaces and `CompositeConsumer`. Migrate built-in processors. Mark old `TelemetryConsumer` `@Deprecated(forRemoval=true)`.
3. **PR 3 — Pipeline graph + FanOut + lifecycle parity.** New `Pipeline` builder with `branch`/`fanOut`. Lifecycle methods on every component. Rewrite the demo. Delete old `Pipeline`, `Processor`, `ForwardingConsumer`.
4. **PR 4 — Streaming tap.** `TelemetryTap`, `Flow.Publisher` plumbing, `BackpressureStrategy`, drop counters.
5. **PR 5 — SPI growth.** `ClientTransportConfig` / `ServerTransportConfig`, named provider selection, prepare for `otlphttp` transport (which can land as a separate PR).

Each PR is independently shippable and keeps `./mvnw verify` green.

---

## Resolved open questions

The two expert reports raised twelve open questions. Reconciled answers:

1. **Should `partial_success` survive a streaming reshape?** Yes. The OTel expert's view wins: fan-out aggregation uses **max** rejected count, not sum (`fanOutMerge`), because all peers received the same batch from the original sender's viewpoint. Sequential merging (`sequentialMerge`) sums because different items can be rejected at different stages. The architect's sum-based `and()` was the wrong arithmetic.

2. **Is OTLP `partial_success` a legitimate back-pressure signal?** Partially. Use `partial_success` when items can be precisely identified as dropped; use gRPC `RESOURCE_EXHAUSTED` (transport-level, retryable) when the whole batch couldn't be queued. Don't conflate them.

3. **Model OTLP as if it were already streaming?** No — keep unary semantics in v1. But the `Consumer<T>.consume() -> CompletionStage` shape is **portable** to OTLP/Arrow's bidirectional streaming when it lands; we don't have to reshape the API again.

4. **`Connector` first-class or just "a sink that publishes onto another sink"?** First-class, as the OTel expert argued. The named role matters for introspection, pipeline routing, and Collector terminology compatibility. Implementation is trivial: `Connector<I, O>` interface plus the existing `CountConnector` split per source signal.

5. **`BatchProcessor` per-signal limits or unified?** Per-signal — and stronger: per-signal type-parameterised `BatchingProcessor<T>`. Cross-signal batching was never meaningful and the unified class was hiding four independent buffers anyway (`processor/BatchProcessor.java:26-40`).

6. **Profiles experimental?** Yes. `@Experimental` annotation; same `Consumer<ProfilesData>` family; users opt in by reading the annotation in Javadoc and KDoc. No separate envelope.

7. **Sync `ExportResult` vs `CompletionStage<ConsumeResult>`.** Async wins. Virtual threads make `stage.toCompletableFuture().join()` effectively free at the call site; fan-out is impossible without it. Synchronous helpers will be added if benchmarks justify them, not preemptively.

8. **`Flow.Publisher<T>` in the public API surface or behind a `Tap` helper?** Behind `TelemetryTap`. The OTel expert is right that raw `Flow.Publisher` semantics are easy to misuse; the tap layer adds `BackpressureStrategy`, drop counters, and a clear "this is decoupled from the in-pipeline ack" contract. The `Flow.Publisher` is exposed as the *return type* of `tap().traces()` etc., so coroutine and Reactor adapters work; the user-facing helper sits on top.

9. **Per-signal `Consumer<T>` family or stay unified.** Per-signal foundation. Compositional unified types (`CompositeConsumer`, `Telemetry` sealed envelope on the tap) sit on top. This kills the silent-drop class of bugs at compile time.

10. **`ConsumeResult` sealed + parameterised, or simple POJO.** Sealed + parameterised. Without `<T>`, fan-out aggregation and per-signal partial_success fidelity quietly break (the `BatchProcessor.flush()` bug).

11. **Receiver SPI shape — class hierarchy or factory + config record.** Factory + record config (`ClientTransportConfig`/`ServerTransportConfig`) at the SPI level; user-facing builders on `OtlpGrpcReceiver`/`OtlpGrpcExporter`. This is the compromise the Java architect and OTel expert converged on.

12. **Transport SPI scope.** Sealed `TransportOption` family on top of the record config. Headers, TLS, compression, retry are first-class fields on the record; transport-specific extensions go through `TransportOption`.

---

## What this does *not* fix

Honest non-goals so the proposal is bounded:

- **OTLP/HTTP exporter and receiver.** The SPI accommodates them; the implementation is a separate workstream.
- **TLS implementation.** `ServerTransportConfig.Tls.Custom` exists in the SPI; the gRPC transport will start with `Tls.Disabled` and add real TLS in a follow-up.
- **In-process OpenTelemetry-Java interop.** Out of scope here — a separate `otlp4j-opentelemetry-bridge` module would expose otlp4j receivers as `LogRecordExporter` / `SpanExporter` to upstream SDK users.
- **Sampler abstraction.** Sampling stays as "use `filter` with a probabilistic predicate". A first-class `Sampler` interface can land later if the use case justifies it.
- **OTLP/Arrow.** Designed-to-be-portable, not implemented.

---

## Acceptance criteria

A reviewer should be convinced when:

- The four per-signal `Consumer<T>` interfaces are present, `TelemetryConsumer` is gone, and the type system rejects wiring a `LogConsumer` as the terminal of a metrics pipeline at compile time.
- The demo no longer contains a hand-rolled anonymous `TelemetryConsumer` for fan-out; it uses `Pipeline.from(...).branch().fanOut(...).join()`.
- `OtlpGrpcReceiver.tap().traces().asFlow().collect { ... }` works from Kotlin and survives a deliberately slow subscriber without affecting the receiver's OTLP acknowledgement to the sender.
- `BatchingProcessor` flushes on age **and** on size, exposes a drop counter, returns `CompletionStage<Void>` from `forceFlush` and `shutdown`, and has tests proving none of the four signals can be cross-mixed.
- `./mvnw verify` is green on every PR in the rollout.
