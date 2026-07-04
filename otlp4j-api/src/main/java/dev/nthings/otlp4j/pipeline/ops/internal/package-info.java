/// Internal batching helpers, excluded from the compatibility promise.
/// [Signal] binds each OTLP signal to its merge strategy and item counter; [BatchMergers]
/// holds the per-signal merge implementations. Both serve [Batcher] only.
@NullMarked
package dev.nthings.otlp4j.pipeline.ops.internal;

import org.jspecify.annotations.NullMarked;
