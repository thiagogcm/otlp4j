# Phase 5 Design (Final): Built-In Components and Declarative Config

This is the implementation-ready, finalized design for Phase 5 of the otlp4j gateway overhaul, the
final phase. It is written against the real tree at HEAD, and every structural claim below has been
re-checked against a concrete file. This finalized revision folds in seven accepted critic findings
(three from the components/core critique, four from the module/build critique); the reasoning for
each acceptance, and the fact that none were rejected, is recorded in the appendix. The writing rule
for this document is full-line-width prose with no em-dashes anywhere.

## 0. Ground truth verified against the tree

Phase 1 through Phase 4 have already landed. The pieces Phase 5 builds on already exist:

- The core contract surface is in `otlp4j-core`: `Component`, `Processor`, `Exporter`, `Receiver`,
  `Connector`, `Extension`, and their factories under
  `otlp4j-core/src/main/java/dev/nthings/otlp4j/core/component/`; `SignalConsumer`,
  `ConsumeContext`, `ClientMetadata`, `ConsumerCapabilities`, `Deadline` under `.../core/consumer/`;
  `TelemetryPayload`, `PayloadView`, `PayloadMemoryMode`, `SignalDescriptor`, `SignalPair` under
  `.../core/signal/`; `DeliveryResult`, `Provenance`, `Disposition`, `DeliveryReason` under
  `.../core/result/`; the neutral policies `BatchPolicy`, `DeliveryQueuePolicy`, `OverflowPolicy`,
  `RetryPolicy`, `BackoffPolicy`, `TimeoutPolicy` under `.../core/policy/`.
- `BatchPolicy` already carries `sendBatchSize`, `sendBatchMaxSize`, `sendBatchMaxBytes`, `timeout`,
  and `metadataKeys`, and `DeliveryQueuePolicy` already carries `queueSize`, `numConsumers`,
  `overflow`, and `waitForResult`. Both are shape-only today (their doc comments say the consuming
  component "lands in a later phase"). Phase 5 is that phase. `AckMode` does not exist in the tree;
  `waitForResult` already subsumes it, exactly as the P1 finding requires.
- `SignalDescriptor.merge(List<T>)`, `itemCount(T)`, and `empty()` are core behaviour over the
  immutable model (`otlp4j-core/.../core/signal/SignalDescriptor.java`). The value set is closed:
  four `public static final` constants and a private constructor, so a descriptor behaves like a
  generic enum. Profiles merge throws `IllegalStateException` on distinct non-empty dictionaries,
  which is why profiles are excluded from batching. Verified: the class exposes only `merge`,
  `itemCount`, `empty`, `batchType`, `signalName`, `wireOrdinal`, `stability`, and `development`;
  there is no inverse split operation today, which is why Phase 5 adds one (section 2.1, accepted
  finding on the batch split primitive).
- The payload contract exposes `itemCount()`, `estimatedSerializedSize()`, `memoryMode()`,
  `view(PayloadView)`, `copyForMutation()`, `writeTo(OutputStream)`
  (`otlp4j-core/.../core/signal/TelemetryPayload.java`). `PayloadView` ships exactly two constants,
  `PROTO_REQUEST` and `DOMAIN_IMMUTABLE`, and its class comment reserves "a routing-metadata view for
  the routing processors and a raw-bytes view" as future constants
  (`otlp4j-core/.../core/signal/PayloadView.java`). Phase 5 adds the routing-metadata constant.
- The two concrete payloads are in `otlp4j-codec`: `ProtoRequestPayload` (proto-backed pass-through,
  materialises the domain lazily) and `DomainRequestPayload` (domain-backed). `Payloads.domain(signal,
  batch)` is the proto-free construction facade; the receive-path proto payload is built by the
  transports through `SignalCodec.<signal>.proto(request)`. `CommonMapper.attributes(List<KeyValue>)`
  converts a proto `Resource`'s attribute list to model `Attributes` without touching spans, metrics,
  or logs (`otlp4j-codec/.../codec/CommonMapper.java:59`), which is the cheap resource-attribute path
  the routing-metadata view needs.
- The service graph is in `otlp4j-service`: `GatewayConfig` (five component maps plus a pipelines map
  plus `ServiceOptions`), `ComponentRegistry`, `GatewayAssembler` (`Gateway.builder()` fluent
  assembler), `GatewayBuilder` (the ten-step validate-and-materialise builder), `GatewayRuntime`
  (topological start and shutdown, extensions started first). The extension component kind exists in
  the registry and lifecycle: `GatewayConfig.extensions()`, `ExtensionEntry`, `ServiceOptions.extensions()`,
  and `GatewayBuilder.instantiate` creates extensions and hands them to `GatewayRuntime`, which starts
  them before the data path and stops them after it. Verified in
  `otlp4j-service/.../service/GatewayBuilder.java` (the extensions loop at the end of `instantiate`)
  and `otlp4j-service/.../service/GatewayRuntime.java` (`start()` starts extensions first).
- The per-signal support capability marker `SignalSupport` currently lives in
  `otlp4j-service/src/main/java/dev/nthings/otlp4j/service/config/SignalSupport.java`, package
  `dev.nthings.otlp4j.service.config`, and is consulted by `ComponentRegistry` through
  `factory instanceof SignalSupport support` (verified at `ComponentRegistry.java:153`). Because it is
  declared in the service module and exported only by `dev.nthings.otlp4j.service`, a component in a
  module that does not require service cannot implement it. Phase 5 relocates it to core so a built-in
  factory can declare it (section 2.1, accepted finding on the module boundary).
- What the graph runtime does with extensions today: it creates them, starts them first, stops them
  last, and exposes them through `GatewayRuntime.component(kind, id)`. Nothing else reaches them. No
  data-path component can look up an extension, because `GatewayBuilder.instantiate` builds the data
  path in the reverse-topological loop first and only then builds extensions, and `CreateContext`
  carries no extension resolver (`otlp4j-core/.../core/component/CreateContext.java` holds only `id`,
  `base`, and `sharedComponents`). This is the exact gap Phase 5 closes so authenticators become
  reachable from receivers and exporters.
- The receiver per-request context is a placeholder: `ReceiverConsumeContext` returns
  `ClientMetadata.empty()`, a fixed 30 second deadline, and never cancels
  (`otlp4j-transport-common/.../transport/common/ReceiverConsumeContext.java`). Its own comment says
  "Capturing the real per-request principal, headers, and deadline from the wire is a later-phase
  concern." Phase 5 is that phase for the principal and headers.
- The receiver dispatch SPI is `Dispatchers.dispatch(TelemetryPayload)`
  (`otlp4j-transport-common/.../spi/Dispatchers.java`), implemented by `ServerReceiver`
  (`.../transport/common/ServerReceiver.java`), which builds the `ReceiverConsumeContext`. The Armeria
  server (`otlp4j-transport-otlp/.../internal/ArmeriaOtlpServer.java`) registers the gRPC services
  (`GrpcServiceAdapters`) and the HTTP routes (`HttpProtocolMapping`) and already has a
  transport-only `ClientAuth` mutual-TLS knob threaded through `OtlpArmeriaReceiver.Builder`. This is
  the pattern the authenticator knob follows.
- The exporter credential seam already exists: `ClientHeaders.resolve(constant, supplier)` overlays a
  per-export header supplier onto the static headers
  (`otlp4j-transport-common/.../transport/common/ClientHeaders.java`), and `OtlpExporterConfig`
  carries the static `headers` map (`.../transport/common/OtlpExporterConfig.java`). Client-side
  credential injection feeds that supplier.
- The samples seed factories are `FunctionProcessorFactory` (a pure-transform processor) and
  `CountConnectorFactory` (a hand-rolled traces-to-metrics connector), both in
  `otlp4j-samples/.../samples/support/`. The count connector's own comment says it "reproduces the
  legacy span-count connector without the full deriving-connector helper, which is a later-phase
  component." Phase 5 ships that helper and rebases the connector onto it.
- `otlp4j-testing` has no `module-info.java` (confirmed: it is absent from the module-info set), so it
  is a classpath module. Its `pom.xml` already declares `otlp4j-core` (optional), `otlp4j-proto`
  (optional), the gRPC and protobuf artifacts (optional), and junit, assertj, awaitility (optional),
  in addition to the single non-optional `otlp4j-model` compile dependency. Crucially, the reactor
  already carries `otlp4j-codec` depending on `otlp4j-testing` at test scope, and
  `otlp4j-service` depending on `otlp4j-codec` at test scope. This is why the conformance kit must not
  add codec or service to `otlp4j-testing`: that would form a reactor cycle (section 5, accepted
  finding on the testing dependency cycle). The wire conformance seam is `GatewayUnderTest`
  (`otlp4j-testing/.../testing/conformance/GatewayUnderTest.java`) with the concrete
  `Otlp4jGatewayUnderTest` in `otlp4j-conformance`.
- The parent reactor lists the modules and manages third-party versions in `pom.xml`. Verified module
  order: model, proto, core, codec, transport-common, service, transport-otlp, samples, testing,
  conformance, coverage. There is no YAML library anywhere in the tree today (grep for snakeyaml,
  org.yaml, and jackson returns nothing in the source tree). The `.m2` cache does hold snakeyaml
  2.0 through 2.6, and snakeyaml 2.x ships a real `META-INF/versions/9/module-info.class` naming the
  explicit module `org.yaml.snakeyaml` (verified on the 2.3 jar), so it is an explicit module, not an
  automatic one (section 1.2, accepted finding on the snakeyaml module posture). `jgrapht-core` is the
  precedent for isolating a dependency whose own transitive automatic modules are quarantined behind an
  internal package, and the build fails on any compiler or javadoc warning (`failOnWarning`,
  `-Xdoclint:all,-missing`), which every new module and `module-info` must satisfy.
- Coverage is aggregated by `otlp4j-coverage`, whose `pom.xml` hard-codes the modules whose exec data
  it attributes (today: model, core, codec, transport-common, service, transport-otlp, samples,
  conformance, testing). The root `pom.xml` carries a comment enumerating the modules that opt into the
  shared `check-coverage` execution (model, core, codec, transport-common, service, transport-otlp).
  Both lists must be updated for the two new production modules (section 1.3, accepted finding on the
  coverage aggregate).

The Collector reference shapes the report cites are present at
`$HOME/gitbase-data/open-telemetry/opentelemetry-collector`: `processor/batchprocessor/config.go`
(fields `Timeout`, `SendBatchSize`, `SendBatchMaxSize`, `MetadataKeys`, `MetadataCardinalityLimit`),
`internal/memorylimiter/config.go` (`CheckInterval`, `MemoryLimitMiB`, `MemorySpikeLimitMiB`,
`limit_percentage`, `spike_limit_percentage`, soft equals limit minus spike, hard equals limit), and
`extension/extensionauth/server.go` plus `client.go` (the `Server.Authenticate(ctx, sources
map[string][]string)` server interface and the `HTTPClient.RoundTripper` / `GRPCClient.PerRPCCredentials`
client interfaces). The design mirrors these.

The legacy `Batcher` behaviour Phase 5 preserves is recovered from git history
(`otlp4j-api/src/main/java/dev/nthings/otlp4j/pipeline/ops/Batcher.java`, present at `a295b3a~1`,
deleted in `a295b3a`). Its degraded-window circuit breaker is triggered strictly by a failed drain,
not by queue overflow: `onDrainOutcome(failure, batchCount)` sets
`degradedUntilNanos = System.nanoTime() + flushInterval.toNanos()` only when `failure != null`, and
clears it to `0L` on a successful drain; the `queue.offer` overflow path never touches
`degradedUntilNanos`. Phase 5 keys the redesigned degraded window off the same downstream delivery
signal (section 2.2, accepted finding on the degraded-window trigger).

## 1. Module, pom, and JPMS changes

Two new reactor modules, plus additive edits to existing ones.

### 1.1 New module `otlp4j-components`

Per the report's target module table ("Built-in processors and helpers: batch, memory limiter,
attributes/resource transforms, routing, filtering"). Depends only on core and codec, so no
transport, wire, or graph library appears on its surface.

`otlp4j-components/pom.xml`: standard reactor child; depends on `otlp4j-core`, `otlp4j-codec`,
`otlp4j-model` (transitive through core but declared for clarity), `org.slf4j`, `jspecify`; test scope
`otlp4j-testing`, `otlp4j-service` (for the graph-integration harness and tests), junit, assertj,
awaitility. Opts into the shared jacoco `check-coverage` execution like the other production modules,
and is added to `otlp4j-coverage/pom.xml` so its production coverage reaches the project-wide aggregate
(section 1.3).

`otlp4j-components/src/main/java/module-info.java`:

```java
module dev.nthings.otlp4j.components {
    requires transitive dev.nthings.otlp4j.core;   // component, consumer, signal, result, policy types
    requires dev.nthings.otlp4j.codec;             // Payloads.domain and the ROUTING_METADATA supply
    requires org.slf4j;
    requires static transitive org.jspecify;
    exports dev.nthings.otlp4j.components.batch;
    exports dev.nthings.otlp4j.components.memory;
    exports dev.nthings.otlp4j.components.routing;
    exports dev.nthings.otlp4j.components.connector;
    exports dev.nthings.otlp4j.components.auth;
    // dev.nthings.otlp4j.components.internal is NOT exported (the delivery queue, match engine).
}
```

`otlp4j-model` is reached transitively through `requires transitive dev.nthings.otlp4j.core` (verified:
`otlp4j-core/module-info.java` declares `requires transitive dev.nthings.otlp4j.model`). Crucially,
this module does not require `otlp4j-service`, so the `SignalSupport` capability a built-in factory
implements must be reachable from core, not service. Section 2.1 relocates `SignalSupport` into
`dev.nthings.otlp4j.core.component` for exactly this reason; the package is already exported by core
(verified in `otlp4j-core/module-info.java`), so `BatchProcessorFactory implements SignalSupport`
compiles here with only the `requires transitive dev.nthings.otlp4j.core` edge already declared.

### 1.2 New module `otlp4j-config-yaml`

Per the report's Phase 5 deliverable ("YAML config loader mirroring the Collector configuration
structure"). Isolated in its own leaf module so a new third-party runtime dependency and its whole
dependency surface stays out of the module graph of core, service, and the transports. This is a
blast-radius decision (keeping the YAML parser off every other module's runtime surface), and it
follows the same isolation instinct behind quarantining `jgrapht-core` inside `otlp4j-service`: there,
the point of the quarantine is that `jgrapht-core` (itself an explicit module) declares
`requires transitive` on two genuine automatic modules, `org.jheaps` and `org.apfloat`, which the
service module then reads two hops away, so the internal graph package keeps those transitive automatic
modules off every consumer's surface (verified: that is exactly what the `otlp4j-service`
`module-info.java` comment records).

`otlp4j-config-yaml/pom.xml`: depends on `otlp4j-core`, `otlp4j-service`, `otlp4j-transport-otlp` and
`otlp4j-components` (to offer `ComponentFactories.standard()` bundling every built-in factory), plus
`org.yaml:snakeyaml`. Test scope adds junit, assertj.

`otlp4j-config-yaml/src/main/java/module-info.java`:

```java
module dev.nthings.otlp4j.config.yaml {
    requires transitive dev.nthings.otlp4j.core;
    requires transitive dev.nthings.otlp4j.service;
    requires dev.nthings.otlp4j.transport.otlp;
    requires dev.nthings.otlp4j.components;
    requires org.yaml.snakeyaml;
    requires org.slf4j;
    exports dev.nthings.otlp4j.config.yaml;
}
```

No `@SuppressWarnings("module")` is placed on this declaration. snakeyaml 2.x is an explicit named
module, not an automatic one: the 2.3 jar in the `.m2` cache carries a real
`META-INF/versions/9/module-info.class` declaring the module `org.yaml.snakeyaml` with its own
`exports` and `requires` clauses (verified by inspecting the jar), and it has no
`Automatic-Module-Name` manifest entry. A `requires org.yaml.snakeyaml;` against an explicit module
raises no `failOnWarning` module warning, so the suppression the earlier draft added on the strength of
an "automatic module" assumption is both unnecessary and based on a false premise. If the compiler ever
does flag a specific warning here, the correct response is to name and suppress that specific warning,
not to blanket-suppress a nonexistent automatic-module concern.

YAML library decision: **snakeyaml 2.x**, pinned to **2.3** (`org.yaml:snakeyaml:2.3`, present in the
local cache and shipping the explicit module descriptor above). It is a single jar with no transitive
dependencies (its own module descriptor requires only `java.base`, with static `java.desktop` and
`java.sql`), is the parser Collector operators already recognise, and provides a `SafeConstructor` that
yields only `Map`, `List`, and scalar types, so no arbitrary-type deserialisation is possible. Because
it is an explicit module with no automatic-module transitive dependencies of its own, it does not even
carry the kind of quarantine hazard `jgrapht-core` does; the leaf module is justified purely on keeping
a third-party runtime dependency out of the other modules' graphs. `jackson-dataformat-yaml` is
rejected: it drags `jackson-core` and `jackson-databind` into the graph for no gain over a bounded
config parse. `snakeyaml-engine` (YAML 1.2) is a reasonable alternative but is less familiar to
Collector users; the design pins `snakeyaml` and the manifest version property `snakeyaml.version` in
the parent `pom.xml` `dependencyManagement`.

### 1.3 Edits to existing modules

- `pom.xml` (parent): add `<module>otlp4j-components</module>` and `<module>otlp4j-config-yaml</module>`
  to `<modules>` (placed after `otlp4j-transport-otlp` and before `otlp4j-samples`, respecting the
  reactor dependency order), add the two reactor artifacts to `dependencyManagement`, add a
  `snakeyaml.version` property (2.3) plus its managed dependency, and update the `check-coverage`
  module-list comment (currently enumerating model, core, codec, transport-common, service,
  transport-otlp) to include `otlp4j-components` and `otlp4j-config-yaml`, both of which opt into the
  shared 90 percent instruction and 80 percent branch floor.
- `otlp4j-coverage/pom.xml`: add `<dependency>` entries for `otlp4j-components` and
  `otlp4j-config-yaml` so `report-aggregate` attributes their production coverage in the project-wide
  report (mirroring the existing hard-coded module list).
- `otlp4j-core/module-info.java`: add `exports dev.nthings.otlp4j.core.config;` and
  `exports dev.nthings.otlp4j.core.auth;` for the two new core packages. The routing-metadata type is
  added to the already-exported `dev.nthings.otlp4j.core.signal` package, and the relocated
  `SignalSupport` lands in the already-exported `dev.nthings.otlp4j.core.component` package, so neither
  needs a new export.
- `otlp4j-service/.../config/SignalSupport.java`: deleted; the type moves to
  `otlp4j-core/.../core/component/SignalSupport.java` (section 2.1). `ComponentRegistry` and the two
  service tests that name it (`ValueTypesTest`, `SharedExporterInstanceTest`, `Fakes`) change their
  import to `dev.nthings.otlp4j.core.component.SignalSupport`; the `instanceof SignalSupport` check at
  `ComponentRegistry.java:153` is otherwise unchanged.
- `otlp4j-codec/module-info.java`: no change. The routing-metadata supply is added inside the existing
  payload classes, and `dev.nthings.otlp4j.codec` is already exported.
- `otlp4j-transport-common/module-info.java`: no new exports. `Dispatchers` and `ReceiverConsumeContext`
  changes are internal to already-exported packages.
- `otlp4j-transport-otlp/module-info.java`: no new requires. The auth interceptor uses `io.grpc` and
  `com.linecorp.armeria`, both already required.
- `otlp4j-testing/pom.xml`: no edit. The conformance kit's abstract bases are core-only and compile
  against the `otlp4j-core` optional dependency and the junit, assertj, awaitility optional
  dependencies that are already declared. Adding `otlp4j-service` or `otlp4j-codec` here is rejected
  because `otlp4j-codec` already depends on `otlp4j-testing` at test scope, so either addition forms a
  Maven reactor cycle (section 5). Anything in the kit that needs the graph runtime or codec payload
  construction lives in `otlp4j-components` test sources instead.
- `otlp4j-samples/pom.xml`: add a compile-scope dependency on `otlp4j-components` (Lot 7, when
  `OtlpE2eDemo` first references the built-in `SpanCountConnectorFactory`), a compile-scope dependency
  on `otlp4j-config-yaml` (Lot 12, when the YAML samples arrive), and a test-scope dependency on
  `otlp4j-testing` (Lot 12, so `ThirdPartyComponentTest` can extend the conformance bases). Without
  these pom entries the samples module-info `requires` clauses have nothing to resolve against and the
  module fails to compile.
- `otlp4j-samples/module-info.java`: add `requires dev.nthings.otlp4j.components;` (Lot 7) and
  `requires dev.nthings.otlp4j.config.yaml;` (Lot 12), each in the same lot as its matching pom
  dependency so every lot compiles.

## 2. Deliverable 1: the otlp4j-components module

### 2.1 Core additions that the components need (all additive, core-only)

These land first (Lot 1) so every component and the loader can name them.

**Relocating `SignalSupport` into core.** The optional per-signal capability marker
`SignalSupport` moves from `dev.nthings.otlp4j.service.config` to `dev.nthings.otlp4j.core.component`
without changing its shape: `boolean supportsSignal(SignalDescriptor<?> signal)`. It must live in core
because a built-in factory in `otlp4j-components` (which does not and must not require `otlp4j-service`)
implements it, while `ComponentRegistry` in `otlp4j-service` still consults it through
`factory instanceof SignalSupport`. Core already exports `dev.nthings.otlp4j.core.component`, so the
move needs no new export, and service reaches the type through its existing
`requires transitive dev.nthings.otlp4j.core`. This is the smallest change that makes the honest
profiles exclusion in `BatchProcessorFactory` (section 2.2) reachable from where the component lives.

**Split primitive on `SignalDescriptor`.** Add a symmetric partition operation mirroring `merge`:

- `otlp4j-core/.../core/signal/SignalDescriptor.java` EDIT: add
  `public List<T> split(T batch, long maxItems)`, backed by a per-signal `Splitter<T>` supplied to the
  four constants exactly as `Merger<T>` and the item counter are today. It partitions one domain batch
  into sub-batches each holding no more than `maxItems` OTLP items (spans, data points, log records, or
  profiles), item-count based, preserving resource and scope grouping. For a batch already at or under
  the cap it returns a singleton list of the batch. This is the inverse of `merge` and is the only
  generically usable mechanism by which `BatchProcessor<T>` can honour `BatchPolicy.sendBatchMaxSize`
  without a per-signal switch inside `otlp4j-components`. It is added to the Lot 1 core-additions list
  and the file inventory (section 7). Profiles split simply slices the profile list under the shared
  dictionary, which is always lossless (unlike profiles merge across distinct dictionaries), so
  `split` is defined for all four signals even though `merge` is hazardous for profiles.

**Routing metadata view.** Add to `PayloadView` a third constant, and a new core return type:

- `otlp4j-core/.../core/signal/PayloadView.java`: add
  `public static final PayloadView<RoutingMetadata> ROUTING_METADATA = new PayloadView<>("ROUTING_METADATA");`.
  This is exactly the "add a new constant only" evolution the class comment already sanctions, so no
  caller and no switch changes.
- `otlp4j-core/.../core/signal/RoutingMetadata.java` (new): the cheap, proto-free routing surface.

```java
public interface RoutingMetadata {
    SignalDescriptor<?> signal();
    long itemCount();
    /// The string value of a resource-level attribute, if present as a string on any resource in the
    /// payload (first match in wire order). This is the common tenancy/service routing key.
    Optional<String> resourceAttribute(String key);
    /// The set of resource-attribute keys present across the payload's resources.
    Set<String> resourceAttributeKeys();
}
```

Exactly what it exposes and why: the routing and filtering processors route on resource-level
attributes (service name, tenant id, deployment environment) and on request headers. `RoutingMetadata`
covers the payload-attribute axis; the header axis is already carried by `ConsumeContext.metadata()`
(`ClientMetadata.headers()`), so the view deliberately does not duplicate it. It exposes only resource
attributes, not span or data-point or log-record attributes, because those cannot be read without
walking the full payload, which defeats "without domain materialisation". `signal()` and `itemCount()`
are surfaced for signal-based routing and for a size guard. The return type is a plain string lookup
(not model `AttributeValue`) because routing keys are string-valued in practice and a string lookup
keeps callers off the model type on the hot path.

**How each payload implementation supplies it** (added in Lot 2, in `otlp4j-codec`, so the components
read it purely through `payload.view(PayloadView.ROUTING_METADATA)`):

- `DomainRequestPayload.view(ROUTING_METADATA)`: the domain batch is already in hand (its memory mode
  is `DOMAIN_BACKED`), so it reads the resource attributes off the domain records directly. A new
  package-private `SignalCodec.routingMetadata(T domainBatch)` walks only the batch's resource list
  (`TracesData.resourceSpans().resource().attributes()`, and the metrics, logs, profiles equivalents)
  and builds a `RoutingMetadata` backed by a merged view of those `Attributes`. No proto is
  materialised.
- `ProtoRequestPayload.view(ROUTING_METADATA)`: the proto request is in hand (`PROTO_BACKED`), and the
  domain is NOT materialised. A new package-private `SignalCodec.routingMetadata(REQ protoRequest)`
  reads only the top-level resource wrappers of the proto message
  (`ExportTraceServiceRequest.getResourceSpansList().get(i).getResource().getAttributesList()`, and
  the three siblings) and converts only those `KeyValue` lists through the existing
  `CommonMapper.attributes(List<KeyValue>)` (verified present at `CommonMapper.java:59`). Spans, data
  points, and log records are never decoded. The proof test asserts `ProtoRequestPayload.domainMaterialised()`
  stays `false` after a `ROUTING_METADATA` read (that test seam already exists on the class).

Both paths produce a `RoutingMetadata` whose `resourceAttribute` and `resourceAttributeKeys` merge the
resources of the payload, so a multi-resource batch answers a routing question once. Item-count parity
between the two supplies is covered by an existing-style parity test (mirroring
`otlp4j-codec/.../PayloadItemCountParityTest.java`).

**Config binding SPI** (Lot 1, core, used by every bindable factory and the YAML loader). New package
`dev.nthings.otlp4j.core.config`, dependency-free (JDK only, no snakeyaml):

- `ConfigNode`: an immutable tree over parsed configuration (map, list, scalar) with typed, key-tracking
  accessors: `Optional<String> string(String key)`, `int intValue(String key, int def)`,
  `long longValue(...)`, `Duration duration(String key, Duration def)`, `boolean bool(...)`,
  `List<String> stringList(String key)`, `Optional<ConfigNode> child(String key)`, `Set<String> keys()`,
  and, crucially, `Set<String> unreadKeys()` which returns the keys never touched by an accessor. This
  makes unknown-key diagnostics free: after a factory binds, the loader reports every `unreadKeys()`
  entry. `ConfigNode` has a static `ConfigNode.of(Map<String,Object> raw)` the YAML module builds from
  snakeyaml output, and an empty node for factories with no config.
- `ConfigBinder<C extends ComponentConfig>`: the optional capability a factory implements to bind its
  own YAML sub-map, `C bind(ConfigNode node, C defaults)`. Type-safe and reflection-free (no `opens`,
  which matters under JPMS with validating record constructors). A factory that does not implement it
  is bindable only with no config keys; any provided key is reported unknown. This mirrors the
  relocated `SignalSupport` optional-capability pattern (now in `dev.nthings.otlp4j.core.component`).

**Extension resolution** (Lot 1 core plus Lot 3 service). New core type and an additive `CreateContext`
field so a data-path factory can look up an extension by id:

- `dev.nthings.otlp4j.core.component.Extensions` (new): `Optional<Extension> get(ComponentId id)` and a
  shared `Extensions.empty()`.
- `CreateContext` (`otlp4j-core/.../core/component/CreateContext.java`): add an `Extensions extensions`
  component. The canonical constructor becomes `(ComponentId id, ComponentContext base, SharedComponents
  sharedComponents, Extensions extensions)`; the existing `(id, base, shared)` and `(id, base)`
  constructors are retained as convenience constructors delegating with `Extensions.empty()`. This is
  source-compatible with every current call site (they use the three-arg and two-arg forms; verified in
  `GatewayBuilder.instantiate` and `Otlp4jGatewayUnderTest`).

**Authenticator SPI** (Lot 1 core; the report's "authenticator extension SPI ... using the extension
component kind reserved in Phase 1"). New package `dev.nthings.otlp4j.core.auth`, JDK-only:

- `ServerAuthenticator extends Extension`: `AuthResult authenticate(AuthSources sources)`, modelled on
  the Collector `extensionauth.Server.Authenticate(ctx, sources map[string][]string)`.
- `AuthSources`: an immutable view of the request's authentication material, `Map<String,List<String>>
  headers()` plus `Optional<Principal> peerCertificatePrincipal()` for the mutual-TLS subject.
- `AuthResult`: a sealed result, `Authenticated(Principal principal, Map<String,List<String>> attributes)`
  or `Denied(String reason)`. The transport maps `Denied` to gRPC `UNAUTHENTICATED` / HTTP `401`.
- `ClientCredentials extends Extension`: `Map<String,String> headers()`, the per-export credential
  headers (a bearer token, a basic auth header). Modelled on the Collector client auth interfaces,
  collapsed to a header supplier because both otlp4j transports inject credentials the same way through
  `ClientHeaders.resolve`.

The SPI lives in core so the transport (which invokes it) and the components (which implement concrete
authenticators) and the loader (which references them) all name it without a new dependency edge; core
is already required everywhere.

### 2.2 BatchProcessor and the delivery queue helper

Package `dev.nthings.otlp4j.components.batch`. Replaces the legacy `Batcher`'s "processor plus sink
plus queue plus scheduler plus overflow plus ack plus circuit breaker" bundle with a batch processor
plus a reusable delivery queue, per the P1 finding.

Files:

- `BatchProcessorFactory` implements `ProcessorFactory<BatchProcessorConfig>`, `SignalSupport` (the
  relocated core capability, section 2.1), and `ConfigBinder<BatchProcessorConfig>`. Type `"batch"`,
  stability `BETA`. `supportsSignal` returns `false` for `SignalDescriptor.PROFILES` and `true` for the
  other three, because profiles merge is not lossless across distinct dictionaries (verified in
  `SignalDescriptor.mergeProfiles`, which throws). This is the honest, validator-visible exclusion; a
  profiles pipeline that lists `batch` fails graph validation with `PROCESSOR_SIGNAL_UNSUPPORTED`
  before start. Because `SignalSupport` now lives in core, the factory implements it with only the
  `requires transitive dev.nthings.otlp4j.core` edge the module already declares.
- `BatchProcessorConfig(BatchPolicy batch, DeliveryQueuePolicy delivery, Duration degradedWindow)`
  implements `ComponentConfig`. `degradedWindow` is a components-level field (not a core policy) that
  carries the legacy degraded-window duration; it is kept off the core policy surface to avoid core
  churn for a single component's tuning. `defaultConfig()` returns a 1 second timeout, 8192-item target,
  a single-consumer queue of 1024 with `OverflowPolicy.BLOCK`, `waitForResult=false`, and a 5 second
  degraded window.
- `BatchProcessor<T>` implements `Processor<T>`. It buffers incoming payloads, keyed by the
  `BatchPolicy.metadataKeys` combination read from `ConsumeContext.metadata().headers()`; empty keys
  and unset metadata are distinct combinations, matching the Collector. Each key combination has its own
  `Batch` accumulator that tracks the accumulated `List<T>` domain batches (read through
  `payload.view(DOMAIN_IMMUTABLE)` cast with `signal.batchType()`), the running item count
  (`payload.itemCount()`), and the running byte size (`payload.estimatedSerializedSize()`). A flush is
  triggered by any of: item count reaching `sendBatchSize`, byte size reaching `sendBatchMaxBytes` (when
  set), or the `timeout` elapsing since the first buffered item. On flush it merges via
  `signal.merge(batches)` and hands the merged domain payload (`Payloads.domain(signal, merged)`) to its
  `DeliveryQueue`. `sendBatchMaxSize`, when set, splits an over-large merged batch into units no larger
  than the cap before enqueue (the Collector split behaviour), using the new item-count-based
  `signal.split(merged, sendBatchMaxSize)` primitive from section 2.1; each returned sub-batch is
  wrapped with `Payloads.domain(signal, sub)` and enqueued in turn. No per-signal switch appears inside
  `otlp4j-components`: the split is entirely generic over `T` through `SignalDescriptor`.
- `internal/DeliveryQueue<T>` (not exported): the reusable delivery boundary. A bounded
  `ArrayBlockingQueue` of `queueSize` (verified as the project's decided queue in the report's Queues
  and Concurrency section), drained by `numConsumers` worker virtual threads that each call
  `next.consume(context, payload)`. Enqueue behaviour:
  - When `waitForResult` is true, the enqueued entry carries a `CompletableFuture<DeliveryResult>` that
    the producing `consume` call returns, completed by the draining worker with the real downstream
    result. This is the wait-for-result semantics that replaces `AckMode`.
  - When `waitForResult` is false, the producer is acknowledged `DeliveryResult.accepted(provenance)`
    at enqueue, and a later drain failure is logged and counted but not signalled upstream.
  - On a full queue, `OverflowPolicy` applies: `DROP_OLDEST` evicts and drops the head, `DROP_NEWEST`
    drops the arriving batch, `BLOCK` applies backpressure (the producer's stage does not complete until
    space frees, observed as latency), `FAIL` returns `DeliveryResult.throttled(retryAfter, ...)`.
  - Degraded-window ingress refusal, keyed off downstream delivery health (not off queue fullness):
    each drain worker reports its downstream outcome after calling `next.consume(...)`, mirroring the
    legacy `BatchDrainEngine.onDrainOutcome`. On a failed or rejected downstream delivery the queue
    records `degradedUntilNanos = now + degradedWindow`; on a successful delivery it clears the marker
    to the healthy sentinel. While the marker is in the future, new enqueues are refused with
    `DeliveryResult.throttled(retryAfter, "delivery queue degraded", provenance)`, admitting a probe
    batch once the window expires. This preserves the legacy `Batcher` degraded-window circuit breaker
    faithfully: the breaker protects against a genuinely failing downstream, and it is a separate axis
    from the `OverflowPolicy` handling of a full buffer. A bursty producer against a healthy but slow
    downstream that merely saturates the queue triggers `OverflowPolicy` (backpressure or drop) but does
    not trip the degraded breaker, and a downstream that fails every delivery trips the breaker even
    when the queue is not full, exactly as the legacy code behaved.
- `internal/Sizer`: an enum over item count and byte size that selects which running total a trigger
  reads; internal because the payload already exposes both counts and no core `Sizer` type is needed.

Lifecycle: `start` creates a single daemon `ScheduledExecutorService` for the per-batch flush timers and
starts the delivery queue's drain workers. `forceFlush` flushes every buffer within the flush deadline
and drains the queue. `shutdown` flushes, stops accepting, drains the queue into the still-open
downstream (which the topological shutdown order guarantees is still up, per `GatewayRuntime.shutdown`),
and stops the scheduler. Capabilities are `nonMutating` (it produces new merged batches, never mutating
inputs, exactly as `FunctionProcessorFactory` reasons about its immutable transforms).

No `SendingQueue` is built, per the explicit P1 non-goal: the `DeliveryQueue` is the single queueing
point.

### 2.3 Memory limiter processor

Package `dev.nthings.otlp4j.components.memory`. Modelled on the Collector's
`internal/memorylimiter/config.go` and `memorylimiter.go`. Shipped as a processor (the primary form),
so it can refuse ingress from the pipeline head; the report allows "processor or extension", and the
processor is the deployable choice because only a data-path component can return a refusal that the
receiver maps to a retryable wire status.

Files:

- `MemoryLimiterProcessorFactory` implements `ProcessorFactory<MemoryLimiterConfig>`,
  `ConfigBinder<MemoryLimiterConfig>`. Type `"memory_limiter"`, stability `BETA`, supports all four
  signals (it inspects nothing in the payload).
- `MemoryLimiterConfig(Duration checkInterval, long limitBytes, long spikeLimitBytes, Duration
  retryAfter)` implements `ComponentConfig`. The soft threshold is `limitBytes - spikeLimitBytes` and
  the hard threshold is `limitBytes`, exactly the Collector's soft-equals-limit-minus-spike and
  hard-equals-limit model (verified in `internal/memorylimiter/config.go`). Validation rejects a zero
  or negative `checkInterval`, a zero `limitBytes`, and a `spikeLimitBytes` not smaller than
  `limitBytes`, matching the Collector's `Validate`.
- `internal/MemoryLimiter`: the shared limiter core (state machine over a memory gauge), reusable so a
  future extension form wraps the same logic. It holds a `LongSupplier usedBytes` gauge (defaulting to
  `Runtime.getRuntime().totalMemory() - freeMemory()`, injectable so tests are deterministic without
  real heap pressure), the two thresholds, and a periodically sampled state (`NORMAL`, `SOFT`, `HARD`)
  refreshed every `checkInterval` on the component's `ScheduledExecutorService`.
- `MemoryLimiterProcessor<T>` implements `Processor<T>`. On `consume`: in `HARD` state it refuses with
  `DeliveryResult.throttled(retryAfter, "memory hard limit exceeded", provenance)` (retryable, so the
  receiver emits gRPC pushback / HTTP `Retry-After`, mapped by the existing
  `GrpcServiceAdapters`/`HttpProtocolMapping`); in `SOFT` state it requests a best-effort GC no more
  than once per check interval and forwards the payload; in `NORMAL` state it forwards. Recovery is
  automatic: once a later sample reads used memory back under the soft threshold, the state returns to
  `NORMAL` and payloads are admitted again. Capabilities `nonMutating`.

### 2.4 Routing and filtering processors

Package `dev.nthings.otlp4j.components.routing`. They consume the `ROUTING_METADATA` view and the
`ConsumeContext.metadata()` headers, and honour the single-downstream processor contract
(`ProcessorFactory.create(ctx, config, signal, next)` gives exactly one `next`, verified in
`otlp4j-core/.../core/component/ProcessorFactory.java`). Multi-output graph nodes are a Phase 1
rejected shape (`GatewayBuilder` rejects `CONNECTOR_MULTIPLE_DISTINCT_PAIRS`), so a built-in processor
forwards to one downstream. Routing to multiple destinations is composed from fan-out plus one branch
filter each, which the graph already supports (`GatewayBuilder.insertFanOuts` inserts a fan-out when a
producer has more than one downstream).

Files:

- `internal/MatchRule`: the shared match engine. A rule is a conjunction of predicates over a
  `RoutingMetadata` resource attribute value and a `ClientMetadata` header value, each an exact-match or
  regex-match on a named key, plus an optional domain predicate escape hatch. `MatchRule.matches(payload,
  context)` reads `payload.view(ROUTING_METADATA)` and `context.metadata()`, so a resource-attribute or
  header rule never materialises the domain; only a domain predicate rule forces a
  `payload.view(DOMAIN_IMMUTABLE)`.
- `FilterProcessorFactory` implements `ProcessorFactory<FilterConfig>`, `ConfigBinder<FilterConfig>`.
  Type `"filter"`, stability `ALPHA`, all four signals. `FilterConfig(MatchSpec include, MatchSpec
  exclude)` where a spec is a list of key/value or key/regex rules. `FilterProcessor<T>`: keeps the
  payload (forwards to `next`) when it matches include and does not match exclude, and drops it
  otherwise, returning `DeliveryResult.accepted(provenance)` with nothing forwarded (the drop-is-accepted
  semantics `FunctionProcessorFactory` already uses for a null transform result). Capabilities
  `nonMutating`.
- `RoutingProcessorFactory` implements `ProcessorFactory<RoutingConfig>`, `ConfigBinder<RoutingConfig>`.
  Type `"routing"`, stability `ALPHA`. `RoutingProcessor<T>` is the branch selector used at a fan-out
  route: it forwards the payload to `next` only when its rule matches, otherwise drops it, so N branch
  copies of the routing processor (each with its own rule, all fed by one fan-out) send each payload to
  exactly the branch whose rule it matches. A `defaultRoute` flag makes a designated branch keep the
  unmatched remainder. This is the content-routing story that the single-`next` contract and the
  existing fan-out compose, documented in the samples.

The distinction from filter: filter is a stand-alone keep-or-drop stage in one pipeline; routing is a
branch of a fan-out that partitions a stream by resource attribute or header. They share `MatchRule` but
carry different intent, config shapes, and default handling, matching "routing and filtering processors"
in the report.

### 2.5 DerivingConnector helper

Package `dev.nthings.otlp4j.components.connector`. The report's "DerivingConnector helper for zero/one
derivations". Generalises `CountConnectorFactory` (verified: its comment defers to this helper).

Files:

- `DerivingConnector` (abstract) implements `Connector` and `SignalConsumer<TelemetryPayload>`. It holds
  the `Provenance`, the downstream `SignalConsumer<TelemetryPayload> next`, and the `SignalPair`. Its
  `consume` calls the subclass hook `Optional<TelemetryPayload> derive(ConsumeContext context,
  TelemetryPayload input)`; on `Optional.empty()` it returns `DeliveryResult.accepted(provenance)` with
  nothing forwarded (zero derivation), and on a present result it returns
  `next.consume(context, derived)` (one derivation), so a derived payload the downstream rejects
  surfaces to the input side, exactly as the hand-rolled count connector does today. Default lifecycle
  is no-op; capabilities `nonMutating`.
- `DerivingConnectorFactory<C extends ComponentConfig>` (abstract) implements `ConnectorFactory<C>`,
  wiring the pair and the payload-typed `next` into a subclass-provided `DerivingConnector`.
- `SpanCountConnectorFactory` extends `DerivingConnectorFactory`: the built-in traces-to-metrics count
  connector, deriving a one-point delta-sum metric from each traces batch, the count read from
  `payload.itemCount()` (no domain materialisation for a proto-backed input). Type `"spancount"`,
  supports the pair `TRACES -> METRICS`. This is the production home of the logic the samples prototyped.

The samples `CountConnectorFactory` is rebased onto `DerivingConnector` (Lot 7): the samples version
becomes a thin subclass or is deleted in favour of the built-in `SpanCountConnectorFactory`, and
`OtlpE2eDemo` is updated to wire the built-in one, proving the helper end to end.

## 3. Deliverable 2: the authenticator extension SPI and its wiring

The core SPI is in section 2.1 (`dev.nthings.otlp4j.core.auth`). Here is how it is invoked and how the
principal reaches `ConsumeContext.ClientMetadata`, on both protocols, plus the client-side credential
path. The extension component kind reserved in Phase 1 is where the authenticators live: a
`ServerAuthenticator` and a `ClientCredentials` are each an `Extension` created through an
`ExtensionFactory` and registered in `GatewayConfig.extensions()`, started before the data path by
`GatewayRuntime`.

### 3.1 Built-in authenticator extensions (components.auth)

- `BearerTokenServerAuthenticatorFactory` implements `ExtensionFactory<BearerTokenAuthConfig>`,
  `ConfigBinder<BearerTokenAuthConfig>`. Type `"bearertokenauth"`. Its extension validates the
  `Authorization: Bearer <token>` header against the configured token set and returns
  `AuthResult.Authenticated` with a `Principal` naming the token holder, or `AuthResult.Denied`.
- `BasicAuthServerAuthenticatorFactory` implements `ExtensionFactory<BasicAuthConfig>`: validates HTTP
  Basic credentials against a configured user table.
- `BearerTokenClientCredentialsFactory` implements `ExtensionFactory<BearerTokenClientConfig>`: a
  `ClientCredentials` whose `headers()` returns `{"Authorization": "Bearer " + token}`. A refreshing
  variant re-reads the token per export.

These are ordinary extensions, created by `GatewayBuilder` and startable and stoppable through the
existing lifecycle. No graph change is needed to host them; the change is making them reachable from
the receiver and exporter factories.

### 3.2 Making an extension reachable at component create time (service, Lot 3)

`GatewayBuilder.instantiate` is reordered so extensions are created first, into a `Map<ComponentId,
Extension>`, wrapped in an `Extensions` view. That view is passed into every data-path `CreateContext`
(the new fourth constructor argument). The receiver, exporter, and connector `create` calls therefore
can resolve a referenced authenticator or credential extension by id. The runtime start order is
unchanged (extensions still start first in `GatewayRuntime.start`); only the build order moves, and it
is safe because an extension has no data-path dependency. This is the single service-side change and it
is additive to `GatewayRuntime` (it already receives the extension list).

### 3.3 Server-side: receiver invokes the authenticator per request (transport, Lot 8)

Config surface: `OtlpReceiverConfig` gains two additive fields,
`@Nullable ComponentId authenticator` (the server authenticator extension to attach) and
`List<String> includeMetadataKeys` (the request headers to capture into `ClientMetadata` for
metadata-keyed batching and routing). The neutral record and its `toServerConfig()` map these across; a
null authenticator preserves today's behaviour exactly.

Resolution: `OtlpArmeriaGrpcReceiverFactory.create` and `OtlpArmeriaHttpReceiverFactory.create`
(verified pattern in `OtlpArmeriaGrpcReceiverFactory.java`) resolve `config.authenticator()` through
`ctx.extensions().get(id)`, cast to `ServerAuthenticator`, and hand it to a new
`AbstractReceiverBuilder.authenticator(ServerAuthenticator)` knob (parallel to the existing
`OtlpArmeriaReceiver.Builder.setClientAuth` mutual-TLS knob, which is the established transport-knob
pattern). A configured authenticator that resolves to no extension, or to an extension that is not a
`ServerAuthenticator`, is a build-time `IllegalStateException` with the id named, surfaced before start.

Invocation, HTTP: `ArmeriaOtlpServer.start` wraps each of the four `/v1/...` route services in an
Armeria `DecoratingHttpServiceFunction` when an authenticator is present. The decorator reads the
request headers into `AuthSources`, calls `authenticator.authenticate(sources)`, and on `Denied`
returns `401 Unauthorized` (mapped to the standard OTLP failure text), and on `Authenticated` stores a
built `ClientMetadata` (the principal plus the `includeMetadataKeys` headers plus any authenticator
attributes) as a `RequestContext` attribute, then calls the delegate. `HttpProtocolMapping.handle`
(verified entry point) reads that attribute and passes the `ClientMetadata` to the widened dispatch.

Invocation, gRPC: the four services are built through `GrpcService.builder()` (verified in
`ArmeriaOtlpServer.start`); a `ServerInterceptor` is added via `GrpcService.builder().intercept(...)`.
The interceptor reads the gRPC `Metadata` into `AuthSources`, calls the authenticator, closes the call
with `Status.UNAUTHENTICATED` on `Denied`, and on `Authenticated` stashes the `ClientMetadata` in the
`io.grpc.Context` for the call. The `TraceServiceAdapter.export` and its three siblings (verified in
`GrpcServiceAdapters`) read the current `io.grpc.Context` and pass the `ClientMetadata` to dispatch.

Carrying the principal to `ConsumeContext`: the `Dispatchers` SPI is widened. `Dispatchers.dispatch`
gains a two-argument form `dispatch(TelemetryPayload payload, ClientMetadata metadata)`, with a default
one-argument `dispatch(payload)` delegating with `ClientMetadata.empty()` for callers that have no
metadata. `ServerReceiver` implements the two-argument form and builds the `ReceiverConsumeContext` from
the supplied metadata instead of `ClientMetadata.empty()`; `ReceiverConsumeContext` gains a
`(Clock, ClientMetadata)` constructor. The transport adapters call the two-argument form with the
captured metadata. The result: an authenticated request's principal and selected headers reach every
downstream consumer through `ConsumeContext.metadata()`, which is exactly what makes
`BatchPolicy.metadataKeys` and metadata routing real. Request cancellation stays a no-op unless the
Armeria and gRPC cancellation signals are wired, which is an optional follow-on noted but not required
by Phase 5.

### 3.4 Client-side: exporter injects credentials (transport, Lot 9)

Config surface: `OtlpExporterConfig` gains an additive `@Nullable ComponentId credentials` field. The
exporter factories (`OtlpArmeriaGrpcExporterFactory`, `OtlpArmeriaHttpExporterFactory`) resolve it
through `ctx.extensions().get(id)`, cast to `ClientCredentials`, and install a per-export header supplier
that returns the credential's `headers()`. This feeds the existing `ClientHeaders.resolve(constant,
supplier)` seam (verified present and designed for exactly this), so credentials overlay the static
headers per export, and a refreshing credential yields a fresh token each call. gRPC and HTTP share the
one supplier path because both run on the one Armeria client. Static-header credentials (an API key) can
also be expressed directly as `OtlpExporterConfig.headers` without an extension; the extension path is
for credentials that need lifecycle or refresh.

## 4. Deliverable 3: the YAML config loader

Module `otlp4j-config-yaml` (section 1.2). It parses a Collector-style YAML document into a
`GatewayConfig` through the factory registry and per-component `ConfigBinder`s, then hands the config to
the existing `GatewayBuilder` for graph validation, so YAML never becomes a second construction path: it
compiles to the one `GatewayConfig` every path produces (verified as the design intent in
`GatewayConfig`'s class comment, "Every construction path compiles to this record").

Files (`dev.nthings.otlp4j.config.yaml`):

- `YamlConfigLoader`: the entry point, `GatewayConfig load(Reader yaml, ComponentFactories factories)`
  and a `Path` overload. It parses with snakeyaml's safe API (`new Yaml(new SafeConstructor(new
  LoaderOptions()))`) yielding maps, lists, and scalars only, applies env substitution, builds
  `ConfigNode` trees, and walks the Collector structure.
- `ComponentFactories`: the `type` string to `ComponentFactory` registry, built by the embedder via a
  builder (`register(factory)`), with a convenience `ComponentFactories.standard()` bundling every
  built-in factory (the OTLP receiver and exporter factories from `otlp4j-transport-otlp`, and the
  batch, memory limiter, filter, routing, spancount connector, and authenticator factories from
  `otlp4j-components`). Optional `ServiceLoader` discovery through a `ComponentFactoryProvider` SPI lets
  a third-party jar contribute factories without the embedder naming them, which is the "added without
  touching core modules" mechanism.
- `EnvSubstitution`: resolves `${env:VAR}` and `${env:VAR:-default}` in scalar string values (the
  Collector `${env:...}` syntax), applied while building `ConfigNode`s. An undefined variable with no
  default is a diagnostic. This is the bounded, explicit substitution the report asks for; there is no
  arbitrary expression language, and the design records that decision rather than excluding
  substitution.
- `ConfigDiagnostic` and `ConfigLoadException`: parse-and-bind diagnostics distinct from the graph
  diagnostics in `otlp4j-service` (`Diagnostic`, `GraphValidationException`). The loader collects every
  diagnostic and throws once, before returning a config. Graph-level problems (missing references,
  cycles, connector pairing) are left to `GatewayBuilder`, which already produces them, so the two
  diagnostic layers do not overlap.

Structure parsed (mirroring the Collector and the `GatewayConfig` shape):

```yaml
receivers:
  otlp:
    protocols: { grpc: {}, http: {} }
    auth: bearertokenauth
processors:
  memory_limiter: { check_interval: 1s, limit_mib: 512, spike_limit_mib: 128 }
  batch: { timeout: 1s, send_batch_size: 8192 }
  filter/keep-prod: { include: { resource_attribute: { deployment.environment: prod } } }
exporters:
  otlp/backend: { endpoint: backend:4317, credentials: bearertokenauth-client }
connectors:
  spancount: {}
extensions:
  bearertokenauth: { tokens: [ "${env:INGEST_TOKEN}" ] }
  bearertokenauth-client: { token: "${env:BACKEND_TOKEN}" }
service:
  extensions: [ bearertokenauth, bearertokenauth-client ]
  pipelines:
    traces:
      receivers: [ otlp ]
      processors: [ memory_limiter, batch, filter/keep-prod ]
      exporters: [ otlp/backend, spancount ]
    metrics/derived:
      receivers: [ spancount ]
      exporters: [ otlp/backend ]
```

Binding: for each `type[/name]` entry, `ComponentId.parse` (verified) yields the id, the loader looks up
the factory by `type`, checks its `kind()` matches the section, binds the sub-map through the factory's
`ConfigBinder` (or `defaultConfig()` when the factory implements no binder and the sub-map is empty),
and builds the typed `ReceiverEntry` and siblings. Pipelines: `signal[/name]` maps the signal name to a
`SignalDescriptor` (via `SignalDescriptor.values()` matched on `signalName()`, verified) and the optional
name to `PipelineId.of(signal, name)`. The assembled maps go straight into a `GatewayConfig`.

Diagnostics: an unknown `type` (no registered factory) is an unknown-component diagnostic naming the id;
a config key never read by the factory's binder (from `ConfigNode.unreadKeys()`) is an unknown-key
diagnostic naming the id and the key; an unknown top-level or pipeline key is likewise reported; a kind
mismatch (a receiver type listed under `processors`) is reported. The design deliberately reuses the
existing graph validation for cross-component problems rather than re-implementing it.

## 5. Deliverable 4: the component conformance test kit

The kit splits along the one reactor constraint that matters: `otlp4j-codec` already depends on
`otlp4j-testing` at test scope, and `otlp4j-service` already depends on `otlp4j-codec` at test scope, so
adding `otlp4j-codec` or `otlp4j-service` as a dependency of `otlp4j-testing` would form a Maven reactor
cycle, which is a hard build failure regardless of scope or the `optional` flag. The kit therefore keeps
its reusable, dependency-light abstract bases in `otlp4j-testing` (they name only `otlp4j-core` types,
which is an optional dependency already declared there), and it keeps the graph-integration harness and
the codec-payload construction, which genuinely need `otlp4j-service` and `otlp4j-codec`, out of
`otlp4j-testing`.

Reusable bases and fakes shipped from `otlp4j-testing`, package
`dev.nthings.otlp4j.testing.conformance.component` (classpath module, no `module-info`, verified),
core-only so no cycle forms:

- `RecordingConsumer<T>`: a `SignalConsumer<T>` that records every payload and returns a programmable
  `DeliveryResult`, for asserting what a component forwards. Core types only.
- `FakeExtension` and `FakeServerAuthenticator`: extension stubs for wiring and auth checks, over the
  `dev.nthings.otlp4j.core.auth` and `core.component` types alone.
- `AbstractComponentFactoryConformanceTest`: factory-metadata honesty. `kind()` matches the factory
  interface, `type()` is non-blank, `stability()` is non-null, `defaultConfig()` is non-null and equal
  across calls, and any `SignalSupport` answer is consistent with what `create` accepts. Uses the
  relocated core `SignalSupport`, so it compiles against core alone.
- `AbstractProcessorConformanceTest`, `AbstractExporterConformanceTest`,
  `AbstractConnectorConformanceTest`, `AbstractExtensionConformanceTest`: lifecycle (start then
  forceFlush then shutdown all complete within a deadline, shutdown is idempotent, double close is
  safe); capabilities honesty (a `nonMutating` consumer leaves the input payload identity and content
  unchanged, checked by feeding a shared payload and asserting it is unmodified); delivery-result
  provenance (the returned result's `Provenance` names the component id, kind, and signal). These drive
  the component through the core contracts with an author-provided component and an author-provided
  payload supplied via `provide...()` hooks, so the bases themselves name only core.
- `AbstractPayloadConformanceTest`: for an author shipping a custom `TelemetryPayload`, the full payload
  contract from `TelemetryPayload` (non-negative counts, size and `writeTo` agreement, view materialise
  and cache, `copyForMutation` independence, unknown view throws). Core-only.

Graph-integration harness and codec-dependent fakes, which need `otlp4j-service` and `otlp4j-codec`,
live in `otlp4j-components` test sources (`src/test/java`), where those two modules are already
test-scope dependencies, and are used by the built-in components' own conformance subclasses. They are
not shipped from `otlp4j-testing` precisely to avoid the reactor cycle:

- `FakeReceiverFactory` and `CapturingExporterFactory`: in-memory receiver and terminal exporter for a
  no-network graph, the capturing exporter mirroring the one `OtlpE2eDemo` defines, generalised. The
  capturing exporter and the payload feeder construct payloads through `Payloads.domain`, hence the
  codec dependency.
- `SingleComponentGateway`: assembles a minimal `Gateway.builder()` graph of fake receiver, the
  component under test, and capturing exporter (or a connector's two-pipeline pair), starts it, drives a
  payload through, and exposes what was captured. Built on the real `GatewayBuilder` and
  `GatewayRuntime`, so it exercises real validation and lifecycle order, hence the service dependency.

The reusable abstract bases are proven in the reactor by having the built-in components' own tests (in
`otlp4j-components`) extend them, and the graph-integration path is proven by those same tests driving a
built-in component through `SingleComponentGateway`. Third-party graph integration is demonstrated a
second way in section 6.3, where `ThirdPartyComponentTest` boots an external fixture component through
the real `YamlConfigLoader` to `GatewayBuilder` to `GatewayRuntime` path, so an external author has both
the shipped abstract bases and the declarative boot path available without needing the internal
`SingleComponentGateway` harness.

## 6. Deliverable 5: samples, and Deliverable 6: exit-criteria proof

### 6.1 Service-graph YAML example

`otlp4j-samples/src/main/resources/gateway.yaml`: a Collector-like config with an OTLP receiver, a
`memory_limiter` then `batch` then `filter` processor chain, an OTLP exporter, and a connector-mediated
pipeline pair (the `spancount` connector consuming the traces pipeline and feeding a
`metrics/derived` pipeline exported to the backend). This is the shape shown in section 4.

`YamlGatewaySample` (`otlp4j-samples/.../samples/YamlGatewaySample.java`): loads `gateway.yaml` through
`YamlConfigLoader.load(reader, ComponentFactories.standard())`, builds and starts the `GatewayRuntime`,
and logs the running graph via `GatewayRuntime.describe()` (verified present).

### 6.2 Embedded example

`BuiltinComponentsDemo` (`otlp4j-samples/.../samples/BuiltinComponentsDemo.java`): the programmatic
counterpart to `OtlpE2eDemo`, wiring the built-in `MemoryLimiterProcessorFactory`,
`BatchProcessorFactory`, `FilterProcessorFactory`, and `SpanCountConnectorFactory` through
`Gateway.builder()`, so the embedded and declarative paths demonstrably compile to the same
`GatewayConfig`. `OtlpE2eDemo` is updated (Lot 7) to use the built-in `SpanCountConnectorFactory` in
place of the samples `CountConnectorFactory`; that lot also adds the samples compile dependency on
`otlp4j-components` and the `requires dev.nthings.otlp4j.components;` clause, so the module compiles.

### 6.3 Exit-criteria proof tests

- `YamlPipelineBootTest` (`otlp4j-samples` test source): loads a Collector-like YAML expressing receiver
  then processors then exporter AND the connector-mediated pipeline pair, boots it end to end over two
  real OTLP/gRPC hops (the `OtlpE2eDemo` topology, driven from YAML), and asserts the filtered spans and
  the derived span-count metric reach the backend. This proves the first exit criterion (a Collector-like
  config expresses both pipeline shapes and boots and runs).
- `ThirdPartyComponentTest`: a test-fixture component (for example a trivial `TagProcessorFactory`)
  defined in the test source, registered through `ComponentFactories.register(...)` and referenced from
  a YAML config, boots and processes telemetry without any edit to core, service, codec, or transport
  modules. This proves the second exit criterion (third-party components added without touching core
  modules). The fixture also extends the reusable conformance bases shipped from `otlp4j-testing`,
  proving the kit on a genuinely external component; the samples module gains its test-scope
  `otlp4j-testing` dependency in Lot 12 for exactly this.

## 7. Complete file inventory

New and edited files, grouped by module. New unless marked EDIT.

`otlp4j-core`:
- `.../core/component/SignalSupport.java` (relocated here from `otlp4j-service`)
- `.../core/signal/SignalDescriptor.java` EDIT (add `split(T, long)` and the per-signal `Splitter`)
- `.../core/signal/RoutingMetadata.java`
- `.../core/signal/PayloadView.java` EDIT (add `ROUTING_METADATA` constant)
- `.../core/config/ConfigNode.java`
- `.../core/config/ConfigBinder.java`
- `.../core/config/package-info.java`
- `.../core/component/Extensions.java`
- `.../core/component/CreateContext.java` EDIT (add `extensions` component, keep convenience constructors)
- `.../core/auth/ServerAuthenticator.java`
- `.../core/auth/ClientCredentials.java`
- `.../core/auth/AuthSources.java`
- `.../core/auth/AuthResult.java`
- `.../core/auth/package-info.java`
- `module-info.java` EDIT (export `core.config`, `core.auth`)
- tests: `RoutingMetadataTest`, `ConfigNodeTest`, `ExtensionsTest`, `AuthTypesTest`,
  `CreateContextExtensionsTest`, `SignalDescriptorSplitTest`

`otlp4j-service`:
- `.../service/config/SignalSupport.java` DELETE (moved to core)
- `.../service/config/ComponentRegistry.java` EDIT (import the relocated `SignalSupport`)
- `.../service/config/package-info.java` EDIT (drop the `SignalSupport` reference, now in core)
- `.../service/GatewayBuilder.java` EDIT (build extensions first, thread `Extensions` into
  `CreateContext`)
- `.../service/internal/BuiltExtensions.java` (the `Extensions` implementation over the built map)
- tests: `ValueTypesTest`, `SharedExporterInstanceTest`, `Fakes` EDIT (import the relocated
  `SignalSupport`); `ExtensionResolutionTest` (new); existing `LifecycleOrderTest` and
  `RuntimeBehaviourTest` extended

`otlp4j-codec`:
- `.../codec/ProtoRequestPayload.java` EDIT (supply `ROUTING_METADATA`)
- `.../codec/DomainRequestPayload.java` EDIT (supply `ROUTING_METADATA`)
- `.../codec/SignalCodec.java` EDIT (add `routingMetadata(REQ)` and `routingMetadata(T)`)
- `.../codec/RoutingMetadataSupport.java` (per-signal proto resource walkers, package-private)
- tests: `RoutingMetadataParityTest`, `RoutingMetadataNoMaterialisationTest`

`otlp4j-components` (new module):
- `pom.xml`, `module-info.java`
- batch: `BatchProcessorFactory`, `BatchProcessorConfig`, `BatchProcessor`,
  `internal/DeliveryQueue`, `internal/Sizer`, `package-info`
- memory: `MemoryLimiterProcessorFactory`, `MemoryLimiterConfig`, `MemoryLimiterProcessor`,
  `internal/MemoryLimiter`, `package-info`
- routing: `FilterProcessorFactory`, `FilterConfig`, `FilterProcessor`, `RoutingProcessorFactory`,
  `RoutingConfig`, `RoutingProcessor`, `internal/MatchRule`, `internal/MatchSpec`, `package-info`
- connector: `DerivingConnector`, `DerivingConnectorFactory`, `SpanCountConnectorFactory`,
  `SpanCountConfig`, `package-info`
- auth: `BearerTokenServerAuthenticatorFactory`, `BearerTokenAuthConfig`,
  `BasicAuthServerAuthenticatorFactory`, `BasicAuthConfig`, `BearerTokenClientCredentialsFactory`,
  `BearerTokenClientConfig`, `package-info`
- test sources: the graph-integration harness `SingleComponentGateway`, the codec-dependent
  `FakeReceiverFactory` and `CapturingExporterFactory`, one suite per component, and the
  conformance-base subclasses (section 8 test matrix)

`otlp4j-transport-common`:
- `.../spi/Dispatchers.java` EDIT (add two-arg `dispatch(payload, metadata)` with a default)
- `.../transport/common/ServerReceiver.java` EDIT (implement two-arg dispatch, build context from
  metadata)
- `.../transport/common/ReceiverConsumeContext.java` EDIT (add `(Clock, ClientMetadata)` constructor)
- `.../transport/common/AbstractReceiverBuilder.java` EDIT (add `authenticator(...)` and
  `includeMetadataKeys(...)`)
- `.../transport/common/OtlpReceiverConfig.java` EDIT (add `authenticator`, `includeMetadataKeys`)
- `.../transport/common/OtlpExporterConfig.java` EDIT (add `credentials`)
- tests: `DispatchMetadataTest`, `ReceiverConsumeContextMetadataTest`, config record tests extended

`otlp4j-transport-otlp`:
- `.../transport/otlp/internal/ArmeriaOtlpServer.java` EDIT (install HTTP decorator and gRPC interceptor
  when an authenticator is present)
- `.../transport/otlp/internal/AuthDecorators.java` (the HTTP `DecoratingHttpServiceFunction` and the
  gRPC `ServerInterceptor`, plus the `RequestContext`/`io.grpc.Context` metadata capture)
- `.../transport/otlp/internal/GrpcServiceAdapters.java` EDIT (read captured metadata, pass to two-arg
  dispatch)
- `.../transport/otlp/internal/HttpProtocolMapping.java` EDIT (read captured metadata, pass to two-arg
  dispatch)
- `.../transport/otlp/OtlpArmeriaGrpcReceiverFactory.java` EDIT (resolve authenticator via
  `ctx.extensions()`)
- `.../transport/otlp/OtlpArmeriaHttpReceiverFactory.java` EDIT (same)
- `.../transport/otlp/OtlpArmeriaGrpcExporterFactory.java` EDIT (resolve credentials via
  `ctx.extensions()`)
- `.../transport/otlp/OtlpArmeriaHttpExporterFactory.java` EDIT (same)
- transport `ConfigBinder` implementations on the four factories (Lot 10)
- tests: `AuthAcceptRejectGrpcTest`, `AuthAcceptRejectHttpTest`, `PrincipalReachesContextTest`,
  `ClientCredentialsInjectionTest`

`otlp4j-config-yaml` (new module):
- `pom.xml`, `module-info.java`
- `.../config/yaml/YamlConfigLoader`, `ComponentFactories`, `ComponentFactoryProvider`,
  `EnvSubstitution`, `ConfigDiagnostic`, `ConfigLoadException`, internal snakeyaml-to-`ConfigNode`
  adapter, `package-info`
- tests: `YamlBindingTest`, `UnknownKeyTest`, `UnknownComponentTest`, `EnvSubstitutionTest`,
  `PipelineParsingTest`, `FullConfigTest`

`otlp4j-testing`:
- no `pom.xml` edit (the core-only bases compile against the already-declared optional `otlp4j-core`,
  junit, assertj, awaitility dependencies; adding service or codec would form a reactor cycle)
- `.../testing/conformance/component/`: `RecordingConsumer`, `FakeExtension`, `FakeServerAuthenticator`,
  `AbstractComponentFactoryConformanceTest`, `AbstractProcessorConformanceTest`,
  `AbstractExporterConformanceTest`, `AbstractConnectorConformanceTest`,
  `AbstractExtensionConformanceTest`, `AbstractPayloadConformanceTest`, `package-info` (all core-only;
  the graph-integration harness and codec-dependent fakes live in `otlp4j-components` test sources)

`otlp4j-samples`:
- `pom.xml` EDIT (compile `otlp4j-components` in Lot 7; compile `otlp4j-config-yaml` and test-scope
  `otlp4j-testing` in Lot 12)
- `src/main/resources/gateway.yaml`
- `.../samples/YamlGatewaySample.java`
- `.../samples/BuiltinComponentsDemo.java`
- `.../samples/OtlpE2eDemo.java` EDIT (use built-in `SpanCountConnectorFactory`)
- `.../samples/support/CountConnectorFactory.java` EDIT or delete (rebased onto `DerivingConnector`)
- `module-info.java` EDIT (`requires components` in Lot 7, `requires config.yaml` in Lot 12)
- tests: `YamlPipelineBootTest`, `ThirdPartyComponentTest`, `BuiltinComponentsDemoTest`

`otlp4j-coverage`:
- `pom.xml` EDIT (add `otlp4j-components` and `otlp4j-config-yaml` dependency entries so
  `report-aggregate` attributes their production coverage)

`pom.xml` (parent): EDIT (two new modules, two managed reactor deps, `snakeyaml.version` 2.3 and managed
snakeyaml, and the `check-coverage` module-list comment updated to include the two new modules).

## 8. Test matrix per component

- BatchProcessor: flush on count trigger; flush on byte trigger; flush on timeout; `sendBatchMaxSize`
  split via `SignalDescriptor.split` (an over-large merged batch partitions into item-capped units);
  `metadataKeys` partition into distinct batchers (empty and unset are distinct); merge correctness via
  `SignalDescriptor.merge`; profiles refused at validation via the core `SignalSupport`;
  `waitForResult=true` returns the real downstream result; `waitForResult=false` acks at enqueue;
  overflow `DROP_OLDEST`, `DROP_NEWEST`, `BLOCK`, `FAIL`; degraded-window ingress refusal after a failed
  downstream drain (not after mere queue overflow) and recovery after a successful drain, with a
  separate test confirming a full queue against a healthy downstream trips `OverflowPolicy` but not the
  degraded breaker; forceFlush drains; shutdown flushes into the still-open downstream.
- SignalDescriptor split (core): each signal splits a batch into item-capped units preserving item
  totals; a batch at or under the cap returns a singleton; profiles split under a shared dictionary is
  lossless.
- Memory limiter: normal state forwards; soft state triggers one GC per interval and forwards; hard
  state refuses with a retryable throttled result and retry-after; recovery admits again once used
  memory drops; config validation (zero check interval, zero limit, spike not smaller than limit); the
  injected gauge drives deterministic thresholds.
- Routing and filtering: filter keep and drop by resource attribute with no domain materialisation
  (proto-backed input stays proto, asserted through the payload test seam); filter by client-metadata
  header; filter by domain predicate materialises the domain; drop returns accepted with nothing
  forwarded; routing partitions a fan-out so each payload reaches exactly its matching branch; default
  route keeps the remainder.
- DerivingConnector: derive-zero forwards nothing and returns accepted; derive-one forwards and
  propagates the downstream result; `SpanCountConnectorFactory` derives the correct count from
  `itemCount` without materialising a proto-backed traces input; the rebased samples connector still
  produces the demo's expected metric.
- Authenticator: gRPC accept and reject (`UNAUTHENTICATED`); HTTP accept and reject (`401`); the
  authenticated principal reaches a downstream processor through `ConsumeContext.metadata().principal()`;
  the selected `includeMetadataKeys` headers reach `ClientMetadata.headers()`; a missing or wrong-typed
  authenticator id fails the build with the id named; client-side bearer and basic credential injection
  observed at a programmed backend server; a refreshing credential injects a fresh token per export.
- YAML binding: each built-in component config binds from YAML; an unknown config key reports an
  unknown-key diagnostic naming id and key; an unknown component type reports an unknown-component
  diagnostic; a kind mismatch reports; env substitution resolves `${env:VAR}` and the default form, and
  an undefined variable without a default is a diagnostic; `signal[/name]` pipeline ids parse; a full
  config round-trips to a `GatewayConfig` that boots.
- Conformance kit: the built-in components' suites extend the reusable abstract bases from
  `otlp4j-testing` and pass; the third-party fixture extends them and passes; graph integration through
  the `otlp4j-components`-local `SingleComponentGateway` drives a payload end to end and asserts start
  and shutdown order.

## 9. Sequential coder lots

Twelve lots. Each is a coherent unit that leaves `./mvnw -B verify` green (compile with
`failOnWarning`, all module and javadoc lint clean, jacoco floors met, every prior and new test
passing). Each lot is additive to what precedes it.

- **Lot 1 (core Phase 5 surface).** Relocate `SignalSupport` from `otlp4j-service` into
  `dev.nthings.otlp4j.core.component` and repoint `ComponentRegistry` and the three service tests to
  the new import; add `SignalDescriptor.split(T, long)`; add `RoutingMetadata`; add
  `PayloadView.ROUTING_METADATA`; add `core.config` (`ConfigNode`, `ConfigBinder`); add `Extensions` and
  the additive `CreateContext` fourth argument with retained convenience constructors; add `core.auth`
  (`ServerAuthenticator`, `ClientCredentials`, `AuthSources`, `AuthResult`); export the two new core
  packages. Value-type and split tests. Additive across core plus the mechanical service import move.
  Green.
- **Lot 2 (codec routing supply).** Supply `ROUTING_METADATA` in both payloads through
  per-signal proto and domain resource-attribute extraction reusing `CommonMapper.attributes`. Parity
  and no-materialisation tests. Green.
- **Lot 3 (service extension resolution).** Reorder `GatewayBuilder.instantiate` to build extensions
  first and thread `Extensions` into every data-path `CreateContext`; unchanged runtime start order.
  Resolution test plus the existing service suites still green. Green.
- **Lot 4 (components module skeleton and BatchProcessor).** New `otlp4j-components` module, pom,
  module-info, parent reactor entry, `otlp4j-coverage` dependency entry, and the root pom
  `check-coverage` comment updated for the new module. Batch processor, config, delivery queue (with the
  drain-outcome-keyed degraded window), sizer, and the full batch test matrix including the split and
  the degraded-versus-overflow separation. Green.
- **Lot 5 (memory limiter).** Memory limiter processor, config, shared limiter core with an injectable
  gauge, and the threshold and recovery tests. Green.
- **Lot 6 (routing and filtering).** Match engine, filter and routing processors and configs,
  consuming `ROUTING_METADATA` and `ClientMetadata`, with the routing-correctness and
  no-materialisation tests. Green.
- **Lot 7 (deriving connector and samples rebase).** `DerivingConnector`, `DerivingConnectorFactory`,
  `SpanCountConnectorFactory`; add the samples compile dependency on `otlp4j-components` and the
  `requires dev.nthings.otlp4j.components;` clause; rebase the samples `CountConnectorFactory` and update
  `OtlpE2eDemo` to the built-in connector; derive-zero and derive-one tests and the demo test still
  green. Green.
- **Lot 8 (server-side auth wiring).** Widen `Dispatchers`; `ServerReceiver` and
  `ReceiverConsumeContext` carry metadata; `AbstractReceiverBuilder.authenticator` and
  `includeMetadataKeys`; `OtlpReceiverConfig` fields; Armeria HTTP decorator and gRPC interceptor in
  `ArmeriaOtlpServer` and `AuthDecorators`; receiver factories resolve the authenticator through
  `ctx.extensions()`; accept, reject, and principal-propagation tests on both protocols with a test
  authenticator. Green.
- **Lot 9 (built-in authenticators and client credentials).** The bearer and basic server
  authenticators and the bearer client credentials in `components.auth`; `OtlpExporterConfig.credentials`
  and the exporter factories' credential resolution feeding `ClientHeaders.resolve`; accept and reject
  through the built-in extensions and client-injection tests. Green.
- **Lot 10 (YAML loader).** New `otlp4j-config-yaml` module (snakeyaml 2.3, explicit module, no
  `@SuppressWarnings("module")`), its `otlp4j-coverage` dependency entry, and the root pom
  `check-coverage` comment updated for it; `YamlConfigLoader`, `ComponentFactories` and `standard()`,
  `ComponentFactoryProvider`, `EnvSubstitution`, diagnostics; add `ConfigBinder` implementations to the
  OTLP transport factories and confirm the components factories' binders; the binding, unknown-key,
  unknown-component, env, and pipeline-parsing tests. Green.
- **Lot 11 (conformance kit).** Add the core-only reusable abstract bases and fakes to `otlp4j-testing`
  (no pom edit needed); add the graph-integration `SingleComponentGateway` and the codec-dependent fakes
  to `otlp4j-components` test sources; have the built-in component suites extend the bases and drive the
  harness, exercising the kit in the reactor. Green.
- **Lot 12 (samples and exit-criteria proof).** Add the samples compile dependency on
  `otlp4j-config-yaml`, the test-scope dependency on `otlp4j-testing`, and the
  `requires dev.nthings.otlp4j.config.yaml;` clause; `gateway.yaml`, `YamlGatewaySample`,
  `BuiltinComponentsDemo`; the `YamlPipelineBootTest` end-to-end boot over real OTLP hops with the
  connector pair; the `ThirdPartyComponentTest` proving a fixture component added without touching core
  modules and passing the conformance kit. Green, and both Phase 5 exit criteria are demonstrated.

## Appendix: rejected critiques

No critiques were rejected. All seven defects across the two critiques were verified against the real
tree and accepted; the verification and the resulting change are recorded here.

- Components critique D1 (module boundary, `SignalSupport`), blocker, accepted. Verified that
  `SignalSupport` is declared in `otlp4j-service/.../service/config/SignalSupport.java`, exported only
  by `dev.nthings.otlp4j.service`, and that the `otlp4j-components` module-info requires only core,
  codec, slf4j, and jspecify, so `BatchProcessorFactory implements SignalSupport` could not compile.
  Resolved by relocating `SignalSupport` into the already-exported `dev.nthings.otlp4j.core.component`
  package (fix option b), reachable by both the service consumer and the component producer without any
  component-to-graph dependency edge (sections 0, 1.3, 2.1, 2.2, 7, Lot 1).
- Components critique D2 (degraded-window trigger), major, accepted. Verified from git history
  (`otlp4j-api/.../pipeline/ops/Batcher.java` and `BatchDrainEngine.java` at `a295b3a~1`) that the
  legacy degraded window was set by `onDrainOutcome` only when the downstream drain failed and cleared
  on success, entirely separate from the `queue.offer` overflow path. Resolved by keying the redesigned
  `DeliveryQueue` degraded window off the drain worker's downstream delivery outcome, keeping the
  breaker and the `OverflowPolicy` as two independent axes (section 2.2, section 8, Lot 4).
- Components critique D3 (no split primitive for `sendBatchMaxSize`), major, accepted. Verified that
  `SignalDescriptor` exposes only `merge`, `itemCount`, `empty`, and metadata accessors, with no inverse
  split, so a generic `BatchProcessor<T>` had no way to honour `sendBatchMaxSize` without a per-signal
  switch. Resolved by adding a symmetric `List<T> split(T batch, long maxItems)` to `SignalDescriptor`,
  budgeted into the Lot 1 core additions and the file inventory (sections 0, 2.1, 2.2, 7, 8, Lot 1).
- Build critique D1 (testing dependency cycle), blocker, accepted. Verified that `otlp4j-codec` depends
  on `otlp4j-testing` at test scope and `otlp4j-service` depends on `otlp4j-codec` at test scope, so
  adding either as a dependency of `otlp4j-testing` forms a Maven reactor cycle. Resolved by keeping the
  core-only abstract conformance bases in `otlp4j-testing` (no pom edit) and relocating the
  graph-integration harness and codec-payload fakes into `otlp4j-components` test sources, with the
  third-party graph proof running through the declarative YAML boot path instead (sections 0, 1.3, 5, 7,
  Lot 11).
- Build critique D2 (samples pom and lot sequencing), blocker, accepted. Verified that
  `otlp4j-samples/pom.xml` declares no dependency on `otlp4j-components` or `otlp4j-config-yaml`, and
  that the earlier draft placed the samples module-info edit five lots after `OtlpE2eDemo` first
  references the built-in connector, so that lot could not compile. Resolved by adding explicit
  `otlp4j-samples/pom.xml` edits and splitting them across lots: the `otlp4j-components` compile
  dependency and its `requires` clause move to Lot 7, and the `otlp4j-config-yaml` compile dependency,
  the test-scope `otlp4j-testing` dependency, and their clauses land in Lot 12 (sections 1.3, 6.2, 6.3,
  7, Lot 7, Lot 12).
- Build critique D3 (snakeyaml module posture), major, accepted. Verified that the snakeyaml 2.3 jar in
  the local cache ships a real `META-INF/versions/9/module-info.class` declaring the explicit module
  `org.yaml.snakeyaml` with no `Automatic-Module-Name`, so it is an explicit module, and that the
  `otlp4j-service` module-info comment actually records the reverse of the earlier draft's
  characterization (jgrapht-core is itself explicit; its transitive `org.jheaps` and `org.apfloat` are
  the automatic modules). Resolved by dropping the `@SuppressWarnings("module")`, pinning snakeyaml 2.3,
  justifying the leaf module on dependency-surface blast-radius grounds, and correcting the jgrapht
  comparison (section 1.2, Lot 10).
- Build critique D4 (coverage aggregate), major, accepted. Verified that `otlp4j-coverage/pom.xml`
  hard-codes the aggregated module list and the root `pom.xml` comment hard-enumerates the
  `check-coverage` modules, and that neither was touched for the new modules. Resolved by adding
  `otlp4j-components` and `otlp4j-config-yaml` as `otlp4j-coverage` dependencies, updating the root pom
  comment, and deciding that both new production modules opt into the shared 90 percent instruction and
  80 percent branch floor (sections 0, 1.1, 1.3, 7, Lot 4, Lot 10).
