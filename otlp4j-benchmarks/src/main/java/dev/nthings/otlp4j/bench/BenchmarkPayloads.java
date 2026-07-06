package dev.nthings.otlp4j.bench;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import java.util.ArrayList;
import java.util.List;

/// Builds the domain batches the benchmarks export, sized small and large, once and immutably.
///
/// A small batch carries a single item and a large batch carries [#LARGE_ITEM_COUNT] items, so the
/// baseline captures both the fixed per-request overhead and the per-item marshalling and forwarding
/// cost. All builders are pure and the returned records are immutable, so a benchmark can hold one
/// instance for the whole trial and hand the same batch to every measured operation.
public final class BenchmarkPayloads {

    /// Number of items in a large batch. Chosen large enough to dominate fixed per-request overhead
    /// while keeping a single end-to-end operation well under a millisecond of serialization work.
    public static final int LARGE_ITEM_COUNT = 500;

    private static final Resource RESOURCE =
            Resource.of(Attributes.builder()
                    .put("service.name", "otlp4j-bench")
                    .put("service.namespace", "baseline")
                    .put("deployment.environment", "bench")
                    .build());

    private static final InstrumentationScope SCOPE = InstrumentationScope.of("otlp4j-bench", "1.0.0");

    private BenchmarkPayloads() {
    }

    /// A traces batch with `spanCount` spans.
    ///
    /// @param spanCount the number of spans
    /// @return the traces batch
    public static TracesData traces(int spanCount) {
        var spans = new ArrayList<Span>(spanCount);
        for (int i = 0; i < spanCount; i++) {
            spans.add(Span.builder()
                    .traceId("0102030405060708090a0b0c0d0e0f10")
                    .spanId(String.format("%016x", i + 1L))
                    .name("GET /resource/" + i)
                    .kind(i % 2 == 0 ? Span.Kind.SERVER : Span.Kind.INTERNAL)
                    .startEpochNanos(1_000L + i)
                    .endEpochNanos(2_000L + i)
                    .attributes(Attributes.builder()
                            .put("http.request.method", "GET")
                            .put("http.route", "/resource/" + i)
                            .put("http.response.status_code", 200L)
                            .build())
                    .build());
        }
        return TracesData.of(RESOURCE, SCOPE, spans);
    }

    /// A metrics batch with `metricCount` single-point gauges.
    ///
    /// @param metricCount the number of metrics
    /// @return the metrics batch
    public static MetricsData metrics(int metricCount) {
        var metrics = new ArrayList<Metric>(metricCount);
        for (int i = 0; i < metricCount; i++) {
            var point = new NumberPoint(
                    Attributes.builder().put("shard", (long) i).build(),
                    0L,
                    2_000L + i,
                    NumberPoint.longValue(i),
                    0L,
                    List.of());
            metrics.add(Metric.builder()
                    .name("otlp4j.bench.metric." + i)
                    .unit("1")
                    .description("benchmark gauge " + i)
                    .data(new Metric.Gauge(List.of(point)))
                    .build());
        }
        return MetricsData.of(RESOURCE, SCOPE, metrics);
    }

    /// A logs batch with `recordCount` records.
    ///
    /// @param recordCount the number of log records
    /// @return the logs batch
    public static LogsData logs(int recordCount) {
        var records = new ArrayList<LogRecord>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            records.add(LogRecord.builder()
                    .epochNanos(1_700_000_000_000_000_000L + i)
                    .severity(LogRecord.Severity.INFO)
                    .body("request " + i + " completed")
                    .attributes(Attributes.builder()
                            .put("log.iostream", "stdout")
                            .put("thread.name", "worker-" + (i % 8))
                            .build())
                    .build());
        }
        return LogsData.of(RESOURCE, SCOPE, records);
    }

    /// The item count for a named size (`small` yields 1, `large` yields [#LARGE_ITEM_COUNT]).
    ///
    /// @param size the size label
    /// @return the item count
    public static int itemCount(String size) {
        return switch (size) {
            case "small" -> 1;
            case "large" -> LARGE_ITEM_COUNT;
            default -> throw new IllegalArgumentException("unknown size: " + size);
        };
    }
}
