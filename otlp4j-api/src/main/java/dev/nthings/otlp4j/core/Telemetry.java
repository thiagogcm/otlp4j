package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.receiver.TelemetryTap;

/// A sealed envelope around the four OTLP signals.
///
/// Used by [TelemetryTap#all()] when a subscriber wants every signal through one channel.
/// Exhaustive pattern matching gives Kotlin when and Java switch compile-time coverage.
public sealed interface Telemetry permits Telemetry.Traces, Telemetry.Metrics, Telemetry.Logs, Telemetry.Profiles {

    /// A trace batch.
    record Traces(TracesData data) implements Telemetry {}

    /// A metrics batch.
    record Metrics(MetricsData data) implements Telemetry {}

    /// A logs batch.
    record Logs(LogsData data) implements Telemetry {}

    /// A profiles batch.
    record Profiles(ProfilesData data) implements Telemetry {}
}
