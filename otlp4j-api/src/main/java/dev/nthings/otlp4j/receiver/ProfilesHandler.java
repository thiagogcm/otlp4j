package dev.nthings.otlp4j.receiver;

import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.pipeline.ExportResult;

/// Consumes profiling telemetry delivered to an [OtlpReceiver].
///
/// Return an [ExportResult] to acknowledge the batch; throw to fail at the transport level. See
/// [ProfilesData] for profile-model limits.
@FunctionalInterface
public interface ProfilesHandler {

    ExportResult onProfiles(ProfilesData profiles);
}
