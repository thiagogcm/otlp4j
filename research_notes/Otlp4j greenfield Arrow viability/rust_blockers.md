# Greenfield OTel-Arrow Rust FFM viability: actual blockers and bounded limitations

## 1. Is an in-process Rust-owned native batch backend intrinsically blocked?

### Takeaway

**No fatal architectural blocker found. The backend is worth a bounded proof of concept.** With Java free to expose closeable native batch/session handles and Arrow remaining Rust-owned, public synchronous pdata APIs provide the required decode, encode, batching and processing primitives. This is source-backed feasibility, not proof of a working `cdylib`, FFM implementation, performance advantage or production-ready foreign boundary. Existing Java representation compatibility does not enter this verdict. [pdata/src/lib.rs:8-57][api] [pdata/src/payload.rs:720-781][conversions] [otap/batching.rs:10-32][batching]

### Cited Findings

**Scope and snapshot.** This narrower reinspection used `/home/thiagomoura/gitbase-data/open-telemetry/otel-arrow` at `ddf1675fe65543594ce8149a34eadec5e17d40e8`. Source worktree remained clean. The researcher guide and applicable root/Rust AGENTS instructions were reread. Prior notes are background only; this analysis assumes greenfield API/SPI design, permits breaking changes, and proposes no compatibility shim, Java fallback or compatibility sidecar. Diagnostic observations below are local results, not claims about upstream CI success.

**Public synchronous access is adequate.** pdata reexports `OtapArrowRecords`, `OtlpProtoBytes`, `Producer` and `Consumer`, and exposes its encoders, protobuf types, Arrow dependency, bounded conversion traits and OTAP processing modules. Its manifest has no engine/controller dependency. The standard conversion implementations use ordinary synchronous Rust methods with owned inputs or exclusive mutable references. Tokio/Tonic dependencies do not by themselves require starting a runtime for those functions. The wrapper can own these Rust objects privately and export only C-compatible function arguments and opaque handles. No existing C ABI was found in the earlier source audit, so wrapper exports are new work, not a ready-made upstream interface. [pdata/src/lib.rs:8-57][api] [pdata/Cargo.toml:15-47][manifest] [payload.rs:720-781][conversions] [producer.rs:220-248][producer]

**The optional codec service is also usable without the collector.** `CodecServiceBuilder` accepts a decode policy, creates its own `Arc<Mutex<...>>` codec state, and synchronously decodes admitted data. Its try-lock reports busy/poisoned state instead of permitting simultaneous mutable operations. The service crate depends on pdata/config/views, not on engine/controller. Alternatively the wrapper can use pdata directly and avoid registry/service machinery entirely. [pdata-codec/src/runtime.rs:53-145][runtime] [pdata-codec/Cargo.toml:15-22][codec-manifest]

#### Bounded offline build attempt

Executed once from `/home/thiagomoura/gitbase-data/open-telemetry/otel-arrow/rust/otap-dataflow`, with an external target directory and a 175-second bound:

```sh
timeout 175s env CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/tmp/opencode/otel-arrow-greenfield-check cargo check --offline --locked -p otel-arrow-dfe-pdata -p otel-arrow-dfe-pdata-codec
```

**Outcome: exit 101 before compilation, not timeout and not a pdata source/API compile error.** Cargo tried to resolve `weaver_common` for the workspace's `benchmarks v0.61.0`; the required Weaver Git revision was not checked out in the offline cache. Error: `can't checkout from 'https://github.com/open-telemetry/weaver.git': you are in the offline mode (--offline)`. The workspace includes benchmarks, whose manifest references Weaver; this explains why selecting only two crates still encountered that dependency during resolution. No dependencies were downloaded, no source/config/submodule was changed, and no prototype was written. The exact output is retained at the diagnostic source link. [Local command output:1-10][check-output] [Cargo.toml:1-5,255][workspace] [benchmarks/Cargo.toml:51][benchmark-manifest]

**Nix is not evidence of an intrinsic Windows failure.** pdata unconditionally declares Nix, but the only inspected `nix::` usages are under pdata's collector-validation test module, itself gated by `cfg(all(test, target_os = "linux"))`. Cached Nix 0.31.3 has crate-wide `#![cfg(unix)]`, so on Windows the crate's Unix implementation is excluded, rather than automatically producing an unsupported-platform compile error. The repository also defines Windows dataflow build/test jobs. This removes the proposed Nix-specific no-go; it does not constitute our own Windows shared-library build. [pdata/Cargo.toml:30][manifest] [pdata/src/lib.rs:36-39][test-gate] [validation/collector.rs:91-124][collector-validation] [validation/error.rs:41-42][validation-error] [cached nix/src/lib.rs:44-45][nix-gate] [rust-ci.yml:744-788,852-858][windows-ci]

**The minimal dependency graph is still substantial.** pdata unconditionally pulls DataFusion, Tokio, Tonic, Nix and model-generation helpers; its feature list does not provide a codec-only switch to eliminate these. This can affect build availability, link size and packaging effort. It is not proof that a library must run a collector or cannot be shared-library-linked. The unsuccessful check means actual host compilation, final dependency closure, binary size and platform artifacts remain unvalidated. [pdata/Cargo.toml:15-61][manifest]

### Inferences

**Hard no-go conditions are requirements incompatible with the chosen unchanged APIs, not reasons to abandon greenfield FFM:**

| Requirement, if non-negotiable | What does not work | Classification |
| --- | --- | --- |
| Complete profiles or multivariate OTAP support immediately, with no new Rust implementation | Current signal/encoding/schema machinery cannot deliver that coverage | Feature-specific no-go for unchanged upstream; extending Rust is additional implementation work. [signals] [multivariate-schema] [encode-metrics-end] |
| One unpartitioned batch for arbitrary telemetry size, or one ordinary OTLP output above the supported output bound | Existing ID-width planning and normal protobuf output machinery have finite limits | Requirement-specific no-go; multiple native batches and bounded output requests are viable. [id-overflow] [proto-buffer] |
| Strict rejection of all malformed protobuf while using the default raw/best-effort decoder | That decoder intentionally accepts a valid prefix with malformed nested suffixes | Wrong decoder choice, not an FFM or strict-decoding blocker. [strict-tests] [strict-codec] |
| Total isolation of arbitrary native memory faults/OOM/abort from the JVM | Same-process native execution has no process-isolation boundary | Incompatible with any in-process native topology; ordinary error/panic handling is not complete fault isolation. This is architectural reasoning, not an observed failure. |
| A single independent decoder per incremental OTAP request, one Arrow C Stream for heterogeneous payloads, or unsynchronized simultaneous mutation of one context | Violates stream/schema/ownership contracts | Incompatible topology, readily avoided by clean-slate handle/session design. [consumer] [arrow-stream] [runtime] |

The useful prototype is a new native backend SPI implementation, with Rust-owned closeable batches, bounded ingress/egress and exclusive mutable contexts. A wrapper versioned independently of pre-1.0 upstream APIs, panic barriers, lifetime-safe handle validation and native packaging are necessary engineering, not evidence that the approach is not worth trying. [api] [runtime] [pdata/README.md:3-4][maturity]

### Gaps

- The build did not reach Rust compilation, so neither successful compilation nor a genuine code-level failure was established. No shared-library build, FFM call, platform cross-build, performance test, fuzz run or existing unit-test run was completed.
- An isolated downstream Cargo package could avoid the checkout workspace's benchmark member during local resolution, but that was not attempted because this task forbids writing a prototype or changing manifests. Whether every direct/transitive dependency is cached remains unknown.
- No allocation-failure containment guarantee should be inferred from Rust `Result` APIs or a wrapper's panic barrier. Native input/batch quotas and fuzzing remain necessary.

## 2. What exactly fails for unsupported signals, oversized data and untrusted OTLP?

### Takeaway

The important limits are **per builder counter, per payload ID domain, and per encoded request**, not a blanket 65K-item limit across telemetry. Most oversized ordinary inputs can be partitioned before Arrow construction and processed as multiple native batches, but existing splitters do not break one metric into datapoint subsets and cannot subdivide an indivisible giant log/span/datapoint without new logic or a changed representation. [encode/mod.rs:164-208,240-256,454-499,676-719,902-968][builders] [split.rs:24-60,143-194][split] [otlp/batching.rs:495-516][byte-split]

### Cited Findings

#### Exact limits visible in the inspected implementation

| Data case | Actual limit/failure | Safe interpretation |
| --- | --- | --- |
| Profiles | `SignalType`, `OtlpProtoBytes` and `OtapArrowRecords` expose only logs, metrics and traces | Generated profiles protobuf alone is not a profiles Arrow backend. Wrapper must advertise unsupported, or implement it natively. [config/lib.rs:64-73][signals] [api] |
| Multivariate metrics | OTLP metric builder does not populate the multivariate slot; schema lookup maps `MultivariateMetrics` to `Schema::EMPTY`, which rejects any columns as extraneous | Not standard gauge/sum/histogram/exponential-histogram/summary coverage. Real nonempty multivariate payloads are not currently accepted by the validated batch store. [encode/mod.rs:971-981][encode-metrics-end] [schema/payloads.rs:31-40][multivariate-schema] [schema/schema.rs:250-257][schema-empty] |
| Trace spans | Checked u16 counter increments after every appended span; 65,535 spans succeed on this counter, the 65,536th causes `U16OverflowError` | This is the current builder's successful count ceiling, not the mathematical 65,536-value capacity of u16. [encode/mod.rs:198-208][builders] |
| Trace resource/scope groups | Checked u16 counters increment once per visited group, including empty groups: maximum successful count 65,535 each | Limits must be checked before conversion; merely capping spans does not bound many empty groups. [encode/mod.rs:201-208][builders] |
| Metric objects/resource groups/scope groups | Checked u16 counters: 65,535 each, the next causes `U16OverflowError` | Metric objects are not datapoints. An item-limit that counts only datapoints is insufficient, especially for empty metrics. [encode/mod.rs:717-719,930-933,950-968][builders] |
| Trace events/links; each metric datapoint/exemplar counter | Checked u32 counter increments after append: maximum successful count 4,294,967,295 per counter; the next causes `U32OverflowError` | Counters are independent payload domains, not one global number. Number datapoints/exemplars share their counters across gauges/sums; other metric-kind tables have separate counters. Practical memory/Arrow/output limits arrive much earlier. [encode/mod.rs:164-195,604-628,676-719,902-925][builders] |
| Logs with record attributes | Log IDs are u16 but allocated only when a log has at least one record attribute. `curr_log_id += 1` is unchecked | At most 65,535 attributed logs avoids overflow on every increment. The 65,536th attributed log overflows this increment, rather than returning the checked error used by other builders. This source-visible issue was not reproduced by execution. [encode/mod.rs:454-471][builders] [record/logs.rs:124-130][log-id] |
| Logs without record attributes | The log ID is null and the counter is not incremented | There is no universal 65,535-log-row ceiling from this counter. Other scope/resource, memory and encoding limits still apply. [encode/mod.rs:466-471][builders] |
| Log scope groups | Checked u16 increment: maximum successful count 65,535 | Independent of whether log IDs are null. [encode/mod.rs:480-490][builders] |
| Log resource groups | Resource enumeration index is cast to u16, not checked | Indices 0 through 65,535 are distinct; the 65,537th group aliases index 0. Do not assume an overflow error protects this path. A conservative wrapper can cap all resource groups at 65,535. Source-visible aliasing risk, not an executed corruption demonstration. [encode/mod.rs:255-263][builders] |
| Native concatenation/reindexing | Counts non-null primary IDs and returns `TooManyItems` when count exceeds that ID type's `MAX`; explicitly one less than the theoretical full range | Existing tests assert u16 `MAX` succeeds and `MAX + 1` fails. Do not assume concat promotes OTAP ID widths or automatically emits multiple outputs. [reindex.rs:243-270,2045-2089][id-overflow] |
| Normal pdata OTLP protobuf output | Standard ProtoBuffer constructors and built-in codec clamp output to `1 << 28`, exactly 268,435,456 bytes, or a smaller configured bound. Writes that exceed it return `EncodeFailure::Dropped`, mapped to `Error::Dropped` (`buffer full`) | Return an error and discard partial scratch, not a successful truncated request. This is an OTLP output-buffer bound, not a universal bound on native Arrow batches or OTAP IPC. [otlp/common.rs:349-356,748-779,857-863][proto-buffer] [error.rs:233-234,261-264][buffer-error] [otlp/logs.rs:243-254][logs-output] |
| `make_item_batches` item-limit parameter | High-level API accepts NonZeroU64, but internal split casts it to u32, substituting u32::MAX only when the cast is zero | Restrict native wrapper limits to 1 through 4,294,967,295, and use much smaller policy values. Do not assume larger u64 values saturate correctly. [groups.rs:126-140][groups] |

**Raising a low-level setter is not a valid bypass.** `BoundedBuf::set_limit` itself does not clamp, but placeholder selection and length patching support only 1-4 seven-bit bytes; the constructor/codec clamp is intentional. Blindly raising the limit can exceed the supported length representation. A larger-output requirement needs deliberate encoder changes or a different native encoding implementation, not just a configuration override. [common.rs:578-613,994-1014][length-width] [common.rs:853-863][proto-buffer]

#### Which splitting works, and which does not?

- Public `otap::batching::make_item_batches` splits native batches and concatenates them by item count. Low-level native split follows root/child ID relationships, so logs/spans are split at whole root rows and their related children remain with them. For metrics, it packs whole metric roots by datapoint counts; a single metric exceeding `max_items` is emitted alone above the limit. It does not currently divide that metric's datapoints. [batching] [split.rs:24-56,143-194][split]
- Public `otlp::batching::make_bytes_batches` can split resource/scope wrappers down to individual whole log/span/Metric records before Arrow construction. It preserves unknown wrapper fields by its documented contract, but an indivisible oversized record/header or malformed opaque unit is emitted alone above the requested size. Budgeted variants also intentionally return whole oversized entries when fan-out/overhead ceilings are reached. It is a best-effort packing tool, not a validator or hard maximum guarantee. [otlp/batching.rs:495-516,531-559][byte-split]
- **Decode first, split later does not fix builder overflow.** A request that cannot be converted to a single Arrow batch has to be partitioned at the wire/Prost level before calling `encode_*_otap_batch`, or handled by a new incremental Arrow builder that resets counters between outputs. The existing byte splitter helps ordinary many-record requests but does not independently enforce all group/ID counts. [builders] [byte-split]
- A new native metric datapoint splitter can emit separate output requests carrying the same resource/scope, name/unit/metadata, metric type, temporality and monotonicity, without aggregating or altering individual datapoints. It must keep datapoint attributes, histograms and exemplars together. This is additional engineering, not an intrinsic prohibition. A single datapoint with an enormous histogram/exemplar/attribute payload remains indivisible under that strategy. Likewise a giant log body or one span with too many events/links cannot be fixed by whole-root splitting. These are design deductions from the inspected whole-record split boundaries and child counters. [split] [byte-split] [builders]

#### Untrusted wire and OTLP correctness

**Raw best-effort decoding is not an acceptable full-validation boundary.** An existing test deliberately appends malformed nested framing to valid logs and asserts that best-effort decode succeeds with the original four logs. A separate all-signal test asserts strict decode rejects the malformed request repeatedly, then accepts valid input. These are inspected existing test assertions, not tests executed during this task. They establish a real documented decoder-policy difference, not evidence of loss on valid OTLP. Default `DecodeValidation` is BestEffort, so the new wrapper must choose Strict explicitly. [pdata-codec/src/codecs/otlp.rs:473-530][strict-tests] [pdata-codec/src/decoder.rs:17-30][decode-policy]

**Strict decoding is publicly achievable now.** Use `CodecServiceBuilder::from_global_registry`, `with_decode_policy(DecodePolicy::new(DecodeValidation::Strict))`, `build`, public registry `resolve_decoder`, `ResolvedCodec::admit`, and `CodecService::decode`. Although `OtlpStrictDecoder` itself is private, it is selected through that public policy. Or call public Prost `LogsData::decode`/`MetricsData::decode`/`TracesData::decode`, then the public pdata record encoders, exactly as the strict implementation does. No private API, collector runtime, Java validator fallback or shim is required. [runtime:53-145][runtime] [registry.rs:254-257,334-345][registry] [codecs/otlp.rs:76-106][strict-codec]

**Strict protobuf parsing is not comprehensive telemetry semantic validation.** For example, Prost can decode a bytes field with a wrong-length trace ID; the object view helper converts wrong-length or all-zero IDs to `None`, and the span builder substitutes zero IDs for missing values. Thus Strict alone does not promise rejection of invalid span IDs. Validate required/nonzero trace and span IDs, optional parent/link/exemplar-ID rules, relevant enum/flag policy, histogram count/bounds/bucket consistency, metric temporality constraints, and resource/batch size quotas before conversion when the ingress contract requires those checks. OTAP schema validation checks field names, types, required columns, nulls and nested schema, not all foreign-key or metric-semantic invariants. The latter checks are required scope for a production validator, not verified existing coverage. [proto/common.rs:261-270][id-parser] [proto/trace.rs:174-200][trace-view] [encode/mod.rs:134-146][builders] [schema/schema.rs:286-333][schema-validation]

**Do not manufacture a valid-data corruption verdict from canonical tests.** Existing equivalence helpers normalize absent/default values and compare sorted/deduplicated message sets. That limits the assertions they make; it does not prove that the production conversion drops repeated observations or violates OTLP. This narrow audit did not execute a valid-input loss reproducer. Source-visible special cases requiring focused tests are unchecked attributed-log IDs, log-resource index truncation, and signed span-duration subtraction after u64-to-i64 casts, particularly timestamps crossing the signed boundary. Those are specific domain/overflow concerns, not Java model parity objections. [testing/equiv/canonical.rs:4-10,40-67][canonical] [builders]

### Inferences

An ingress policy with explicit per-domain counters, message/memory budgets, strict parsing and native partitioning is a viable greenfield contract. Rejecting an indivisible record that exceeds the configured supported domain is honest bounded behavior; silently wrapping IDs or returning partial encoded output is not. An API returning a sequence of native batch handles is preferable to promising one arbitrary input always maps to one native batch. [builders] [proto-buffer] [byte-split]

### Gaps

- No actual u16/u32 boundary test or giant-record test was run. u32 ceilings are calculated from checked counter code; the upstream reindex tests explicitly do not allocate u32-scale cases.
- Full valid-OTLP semantic preservation, future unknown-field preservation through Arrow, unsigned timestamp extremes, empty structural groups and invalid-ID normalization were not established end to end. They require a targeted conformance corpus. Group order/default normalization alone is not classified as a bug.
- Memory budgets and Arrow variable-width/list offset constraints add limits beyond these counters. No claim is made that satisfying the counter ceilings permits practical multi-billion-row allocations.

## 3. Is this only a codec, or can native batches do meaningful work without Java Arrow?

### Takeaway

This can be a genuine native batch-processing backend: public pdata operations concatenate/reindex, split/rebatch, filter root selections with child propagation, and transform attributes directly on Rust Arrow records. Keeping batches Rust-owned makes Arrow C Data optional, not a prerequisite; after conversion, these operations need not round-trip through protobuf or Java object models. Native processing is conversion-free with respect to OTLP, not necessarily allocation-free or zero-copy. [batching] [concat] [filter] [attributes]

### Cited Findings

| Public native primitive | What is verified | What must not be promised |
| --- | --- | --- |
| `otap::batching::make_item_batches` | Signal-homogeneous split/reassemble into a vector of OTAP batches | Universal strict item bound for an oversized single metric; automatic per-ID/per-byte budgeting. [batching] [split] |
| `otap::transform::concatenate::concatenate`, `ConcatOptions` | Public function, reindexes unrelated IDs, unifies adaptive schemas/dictionaries, consumes inputs | Arbitrarily large output or immutable input arrays. `preserve_ids` is valid only when decoded IDs are already non-overlapping. [concat:159-249][concat] |
| `otap::transform::split::split` | Public whole-root partitioning with related child batches | A general-purpose datapoint splitter for a single metric, or subdivision of one giant log/span. [split] |
| `otap::filter::filter_otap_batch` | Public Arrow BooleanArray selection, new root batch plus child filtering using `IdBitmapPool` | Arbitrary SQL/query optimization or a general expression engine. Selection length must match root rows; metric root selection keeps/drops whole metrics. [filter.rs:1681-1755][filter] |
| `otap::filter::logs::LogFilter`, `metrics::MetricFilter` | Public filter methods, include/exclude handling; inspected MetricFilter builds a root filter then invokes child propagation | Full OTTL/KQL parity, datapoint-level arbitrary filtering, or measured acceleration. [logs.rs:106-156][log-filter] [metrics.rs:51-93][metric-filter] |
| `otap::transform::apply_attribute_transform` and attribute transform helpers | Public payload-directed attribute changes; rename/delete/insert/upsert/update and stats are implemented | Every possible application transform, or performance superiority over another backend. Transform configuration must be validated. [transform.rs:1086-1142,1381-1399,1773-1790][attributes] |

Concatenation documentation explicitly describes reindexing, schema unification and one destination write per selected output column. It copies/allocates Arrow buffers while staying in Arrow. Root filter fast paths can return a cheap Arc-backed clone or empty storage, while partial filtering allocates selected data. This is meaningful native processing, not merely byte transcoding, but no acceleration claim follows without measurements. [concat.rs:4-76][concat] [filter.rs:1704-1727][filter]

#### Topologies and usage patterns that do not work

| Does not work | Why | Clean-slate solution |
| --- | --- | --- |
| Fresh Consumer for every incremental BAR | Consumer holds IPC reader/dictionary state; later payloads can omit schema and initial dictionaries | One ordered native Consumer session per wire stream; fresh sessions only for independently encoded complete payloads. [consumer] [producer] |
| Parallel mutation of one Producer/Consumer/batch | APIs require exclusive mutable Rust access; service reports contention/poisoning | Serialize session access or create independent contexts. No need for one global context. [producer] [consumer] [runtime] |
| Treating `produce_bar` as an immutable read | It calls `encode_transport_optimized`, sorting/re-encoding batch data before IPC production | Make this exclusive/destructive in the API, or serialize an Arc-backed batch clone; normalize IDs before further operations expecting plain ID relationships. [producer:244-248][producer] [otap.rs:157-165][normalize] |
| `ConcatOptions::preserve_ids` for unrelated inputs | Their local join IDs can collide and attach children to the wrong roots | Default reindexing, with bounded output domains. Preserve IDs only for proven-disjoint pieces. [concat:159-201][concat] |
| One ordinary Arrow C Stream for the entire OTAP batch | A RecordBatchReader stream has one schema; the telemetry batch contains heterogeneous root/attribute/event/datapoint schemas | No Arrow export at all for Rust-owned processing; if later needed, per-payload C Data or separate per-payload streams. [arrow-stream] [pdata/README.md:18-35][pdata-representations] |
| Exposing encoder scratch as an indefinitely live MemorySegment | Next encode/reuse can invalidate the borrow | Owned output-buffer handles, or bounded borrowed access that cannot outlive exclusive context access. [pdata-codec/src/encoder.rs:33-49,69-105][encoded-output] |

Producer has a `reset_streams` method for starting complete IPC streams, but this audit did not qualify all reset/schema-change/old-consumer combinations. The simplest standalone proof should create fresh producer and consumer contexts for each independent exchange; the streaming proof should preserve sessions and explicitly test reconnection/reset behavior. This is stream-session engineering, not an argument against an in-process backend. [producer.rs:314-331][producer]

#### Recommended greenfield proof, without compatibility work

1. Define a backend SPI in terms of owned closeable native batch lists, signal/capability metadata, exclusive codec sessions and owned encoded buffers. Keep every Arrow allocation in Rust and avoid C Data imports, Java callbacks and per-field FFM calls in the first proof.
2. Strictly decode bounded OTLP logs into native handles, perform public native concatenate/reindex plus one root-filter or attribute-transform operation, then encode bounded OTLP and independently decodable BAR output. Demonstrate that the intermediate operations never return to protobuf or Java records. [strict-codec] [concat] [filter] [attributes] [producer]
3. Add traces and standard metrics, then the cases the existing splitters do not guarantee: many empty groups, attributed-log boundary, one huge metric, one indivisible giant record, and output-size failure. Extend native partitioning where required; do not hide unsupported cases behind another Java representation. [builders] [split] [byte-split] [proto-buffer]
4. Exercise a long-lived ordered OTAP producer/consumer session across dictionary/schema changes, explicit ownership/release, encoder mutation and failure recovery. [producer] [consumer] [runtime]
5. Benchmark retained-native batches against their own ingress/egress cost. Add optional Arrow C Data export only if another columnar consumer actually needs it. Arrow's cached `to_ffi`/`from_ffi` helpers exist, but they are irrelevant to same-library Rust-to-Rust processing. [arrow-ffi] [concat]

These steps are proposed experiments, not an implemented wrapper. Protobuf-to-Arrow conversion necessarily reorganizes and writes column buffers; Arrow-to-protobuf necessarily serializes. FFM cannot remove those representation changes. Once native, conversion-free Rust Arrow processing is supported by the public operations above, and avoiding Java materialization gives the architectural experiment a credible purpose. [builders] [conversions] [concat] [filter]

### Inferences

The adjustable work consists of the C ABI/FFM implementation, native-handle SPI, strict/semantic ingress policy, bounded native partitioning, mutation/session/lifetime rules, packaging and measured optimization. Unsupported profiles/multivariate features and missing single-metric splitting are real capabilities to implement or exclude explicitly, not proof the architecture is futile. There is no source-backed reason here to reject even trying a logs/traces/standard-metrics Rust-owned batch backend. [api] [signals] [multivariate-schema] [split]

### Gaps

- Complete filtering/query acceleration was not verified and is not claimed. Only the particular public pdata primitives listed above were inspected closely enough to substantiate native work.
- Arrow C Data is optional for this topology. If later used, release callbacks, schema/dictionary ownership and library unload safety require separate validation; the availability of dependency FFI helpers does not establish a finished OTel-Arrow foreign ABI.
- The first bounded build failed at dependency resolution. A subsequent authorized implementation spike should first establish a compilable isolated dependency set and a functioning shared-library/FFM call, then measure and test correctness. Nothing in this audit establishes that future step as either already successful or intrinsically impossible.

[api]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/lib.rs#L8-L57
[conversions]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/payload.rs#L720-L781
[manifest]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/Cargo.toml#L15-L61
[codec-manifest]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata-codec/Cargo.toml#L15-L22
[runtime]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata-codec/src/runtime.rs#L4-L145
[registry]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata-codec/src/registry.rs#L254-L345
[decode-policy]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata-codec/src/decoder.rs#L17-L30
[strict-codec]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata-codec/src/codecs/otlp.rs#L76-L106
[strict-tests]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata-codec/src/codecs/otlp.rs#L473-L530
[workspace]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/Cargo.toml#L1-L255
[benchmark-manifest]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/benchmarks/Cargo.toml#L45-L53
[check-output]: file:///home/thiagomoura/.local/share/opencode/shell/e5eda3afeb879b5bc7112eb6f528198d7f08c7de/sh_1222a748c001tfDlmXV4qzXRQU.out#L1-L10
[test-gate]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/lib.rs#L32-L42
[collector-validation]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/validation/collector.rs#L91-L124
[validation-error]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/validation/error.rs#L38-L43
[nix-gate]: file:///home/thiagomoura/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/nix-0.31.3/src/lib.rs#L44-L45
[windows-ci]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/.github/workflows/rust-ci.yml#L744-L858
[maturity]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/README.md#L3-L4
[signals]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/config/src/lib.rs#L64-L73
[multivariate-schema]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/schema/payloads.rs#L31-L40
[schema-empty]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/schema/schema.rs#L250-L257
[builders]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/encode/mod.rs#L49-L981
[encode-metrics-end]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/encode/mod.rs#L971-L981
[log-id]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/encode/record/logs.rs#L124-L130
[id-overflow]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap/transform/reindex.rs#L243-L2089
[proto-buffer]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otlp/common.rs#L349-L863
[buffer-error]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/error.rs#L233-L264
[logs-output]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otlp/logs.rs#L204-L254
[length-width]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otlp/common.rs#L578-L1014
[groups]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap/groups.rs#L126-L140
[batching]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap/batching.rs#L10-L32
[split]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap/transform/split.rs#L24-L194
[byte-split]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otlp/batching.rs#L495-L559
[id-parser]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/views/otlp/proto/common.rs#L261-L270
[trace-view]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/views/otlp/proto/trace.rs#L174-L200
[schema-validation]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/schema/schema.rs#L286-L333
[canonical]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/testing/equiv/canonical.rs#L4-L67
[concat]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap/transform/concatenate.rs#L4-L249
[filter]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap/filter.rs#L1681-L1755
[log-filter]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap/filter/logs.rs#L106-L156
[metric-filter]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap/filter/metrics.rs#L51-L93
[attributes]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap/transform.rs#L1086-L1790
[producer]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/encode/producer.rs#L197-L331
[consumer]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/decode/decoder.rs#L23-L116
[normalize]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/src/otap.rs#L157-L165
[encoded-output]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata-codec/src/encoder.rs#L33-L105
[pdata-representations]: https://github.com/open-telemetry/otel-arrow/blob/ddf1675fe65543594ce8149a34eadec5e17d40e8/rust/otap-dataflow/crates/pdata/README.md#L18-L35
[arrow-stream]: file:///home/thiagomoura/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/arrow-array-58.4.0/src/ffi_stream.rs#L153-L175
[arrow-ffi]: file:///home/thiagomoura/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/arrow-array-58.4.0/src/ffi.rs#L264-L284
