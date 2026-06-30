package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.Source;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

/// An ingest endpoint for OTLP telemetry.
///
/// A receiver exposes one [Source] per signal — these are the typed attachment points the
/// pipeline DSL consumes — plus a [TelemetryTap] for side-channel live streaming. Concrete
/// receivers (`OtlpGrpcReceiver`, `OtlpHttpReceiver`, etc.) implement this interface.
public interface Receiver extends Drainable {

    /// The trace ingest source.
    Source<TracesData> traces();

    /// The metrics ingest source.
    Source<MetricsData> metrics();

    /// The logs ingest source.
    Source<LogsData> logs();

    /// The profiles ingest source.
    Source<ProfilesData> profiles();

    /// The receiver's live side-channel.
    TelemetryTap tap();

    /// Starts the underlying transport and returns this receiver for chaining. Throws
    /// [IllegalStateException] if the receiver has already been started.
    Receiver start();

    /// The port the receiver is bound to (gRPC, HTTP, etc.). Zero before [#start] completes.
    int port();

    /// Drains in-flight requests and stops the transport. [#close()] applies the default 10-second
    /// grace; use [#shutdownNow()] for an immediate stop.
    @Override
    CompletionStage<Void> shutdown(Duration timeout);

    /// Forcibly stops the transport.
    CompletionStage<Void> shutdownNow();
}
