package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.ProfilesData;

/// A [Consumer] specialised for profile batches.
@FunctionalInterface
public interface ProfileConsumer extends Consumer<ProfilesData> {}
