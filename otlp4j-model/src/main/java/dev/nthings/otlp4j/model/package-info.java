/// Pure, immutable OTLP domain records shared across the whole SDK.
///
/// Application code reads and constructs these types — `TraceData`, `MetricsData`, `LogsData`,
/// `ProfilesData` and their building blocks (`Resource`, `InstrumentationScope`, `Attributes`,
/// `AttributeValue`, `Span`, `Metric`, `LogRecord`). Every signal batch preserves OTLP's
/// resource/scope grouping and offers a flattened accessor; the `of(...)` factories build the
/// common single-resource/single-scope shape without hand-nesting wrappers. The package depends on
/// neither generated protobuf, gRPC, nor the pipeline API.
package dev.nthings.otlp4j.model;
