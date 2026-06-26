/// Processing building blocks attached as pipeline consumers.
///
/// `Transforms` is the discoverable home for ready-made 1:1 `Transform`s (span and log-record
/// filters, plus per-signal resource-attribute setters). `BatchingProcessor` is a stateful,
/// queue-backed batcher with a configurable `DropPolicy`. Signal-changing derivations instead live
/// in the connector package.
@NullMarked
package dev.nthings.otlp4j.processor;

import org.jspecify.annotations.NullMarked;
