/// Processing building blocks attached as pipeline consumers.
///
/// `Transforms` is the discoverable home for ready-made 1:1 `Transform`s (span and log-record
/// filters, plus per-signal resource-attribute setters). `BatchingProcessor` is a stateful,
/// queue-backed batcher with a configurable `OverflowPolicy`. Signal-changing derivations live in the
/// `Connectors` count sinks (`spanCount`, `logRecordCount`) under a configurable `FailurePolicy`.
@NullMarked
package dev.nthings.otlp4j.processor;

import org.jspecify.annotations.NullMarked;
