package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.ProfilesData;

/// A [Processor] specialised for profile batches - the SAM user code plugs a profile-processing lambda into.
@FunctionalInterface
public interface ProfilesProcessor extends Processor<ProfilesData> {}
