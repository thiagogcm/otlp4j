package dev.nthings.otlp4j.core;

import dev.nthings.otlp4j.model.ProfilesData;

/// A [Sink] specialised for profile batches.
@FunctionalInterface
public interface ProfileSink extends Sink<ProfilesData> {}
