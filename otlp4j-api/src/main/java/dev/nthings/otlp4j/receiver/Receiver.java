package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.Source;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// An ingest endpoint for OTLP telemetry.
///
/// A receiver exposes one [Source] per signal — these are the typed attachment points the
/// pipeline DSL consumes — plus a [TelemetryTap] for side-channel live streaming. Concrete
/// receivers (`OtlpGrpcReceiver`, future `OtlpHttpReceiver`, etc.) implement this interface.
public interface Receiver extends AutoCloseable {

    /// The trace ingest source.
    Source<TraceData> traces();

    /// The metrics ingest source.
    Source<MetricsData> metrics();

    /// The logs ingest source.
    Source<LogsData> logs();

    /// The profiles ingest source.
    Source<ProfilesData> profiles();

    /// The receiver's live side-channel.
    TelemetryTap tap();

    /// Starts the underlying transport. Idempotent on a started receiver.
    Receiver start();

    /// The port the receiver is bound to (gRPC, HTTP, etc.). Zero before [#start] completes.
    int port();

    /// Drains in-flight requests and stops the transport.
    CompletionStage<Void> shutdown(Duration timeout);

    /// Forcibly stops the transport.
    CompletionStage<Void> shutdownNow();

    @Override
    default void close() {
        shutdownNow().toCompletableFuture().join();
    }
}
