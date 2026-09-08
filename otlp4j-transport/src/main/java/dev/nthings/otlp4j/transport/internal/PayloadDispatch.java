package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.signal.Payload;
import java.util.concurrent.CompletionStage;

/// The dispatch seam a wire server invokes as it decodes incoming OTLP requests. The receiver runtime
/// ([dev.nthings.otlp4j.transport.OtlpReceiver]) implements it and hands itself to the server it
/// builds, closing the construction cycle between the receiver and its server; the gRPC and HTTP
/// service adapters invoke it per decoded request. Internal to the transport module: the real
/// extension seam the graph consumes is `ReceiverFactory` in core.
@FunctionalInterface
public interface PayloadDispatch {

    /// Dispatches a decoded payload into the graph, returning the graph's delivery result.
    ///
    /// @param payload the decoded telemetry payload, keyed by its signal
    /// @return a stage completing with the delivery result
    CompletionStage<DeliveryResult> dispatch(ConsumeContext context, Payload<?> payload);
}
