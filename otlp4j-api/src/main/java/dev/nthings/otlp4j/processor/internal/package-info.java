/// Internal batching helpers — not part of the public API and excluded from the compatibility
/// promise. `Signal` binds each OTLP signal to its merge strategy and item counter; `BatchMergers`
/// holds the per-signal merge implementations. Both serve `BatchingProcessor` only.
@NullMarked
package dev.nthings.otlp4j.processor.internal;

import org.jspecify.annotations.NullMarked;
