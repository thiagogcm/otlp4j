package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.model.ProfilesData;

/// A [Sink] specialised for profile batches.
///
/// Build one from a lambda, or from a plain consumer or stage-returning function via
/// [Sink#accepting(ThrowingConsumer)] and [Sink#fromStage(java.util.function.Function)].
@FunctionalInterface
public interface ProfileSink extends Sink<ProfilesData> {}
