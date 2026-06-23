package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/// Service-provider interface for creating [OtlpServer]s.
///
/// Receivers register four per-signal dispatchers; the transport invokes them as decoded
/// OTLP requests arrive. Each dispatcher returns a [CompletionStage] so the transport can
/// hold the request open until the pipeline acknowledges (or partial-succeeds). The high-level
/// receivers select a provider by [#protocol()], so the gRPC and HTTP servers can coexist.
public interface OtlpServerProvider extends TransportProvider {

    /// Creates a server honouring `config` and bound to the four signal dispatchers.
    OtlpServer create(ServerTransportConfig config, Dispatchers dispatchers);

    /// Per-signal dispatch entry points that the transport invokes from its decoded gRPC handlers.
    record Dispatchers(
            Function<TraceData,    CompletionStage<ConsumeResult<TraceData>>>    traces,
            Function<MetricsData,  CompletionStage<ConsumeResult<MetricsData>>>  metrics,
            Function<LogsData,     CompletionStage<ConsumeResult<LogsData>>>     logs,
            Function<ProfilesData, CompletionStage<ConsumeResult<ProfilesData>>> profiles) {}
}
