package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/// Per-signal dispatch entry points an [OtlpServer] invokes as it decodes incoming OTLP requests.
/// Each returns a [CompletionStage] so the transport can hold the request open until the pipeline
/// acknowledges (or partial-succeeds).
public record Dispatchers(
        Function<TracesData,    CompletionStage<ConsumeResult<TracesData>>>    traces,
        Function<MetricsData,  CompletionStage<ConsumeResult<MetricsData>>>  metrics,
        Function<LogsData,     CompletionStage<ConsumeResult<LogsData>>>     logs,
        Function<ProfilesData, CompletionStage<ConsumeResult<ProfilesData>>> profiles) {}
