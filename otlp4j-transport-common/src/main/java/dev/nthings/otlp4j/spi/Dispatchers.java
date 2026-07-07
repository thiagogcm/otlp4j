package dev.nthings.otlp4j.spi;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.concurrent.CompletionStage;

/// Dispatch entry point an [OtlpServer] invokes as it decodes incoming OTLP requests. Returns a
/// [CompletionStage] so the transport can hold the request open until the graph acknowledges (or
/// partial-succeeds).
public interface Dispatchers {

    /// Dispatches a decoded payload into the graph, returning the graph's delivery result.
    ///
    /// @param payload the decoded telemetry payload, keyed by its signal
    /// @return a stage completing with the delivery result
    CompletionStage<DeliveryResult> dispatch(TelemetryPayload payload);
}
