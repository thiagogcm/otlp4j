/// Pipeline identity. `PipelineId` is the Collector pipeline mental model: a signal plus an optional
/// name, in the string forms `traces`, `traces/high-priority`, and `logs/audit`. It names a lane of
/// processing within one signal so a gateway can run several distinctly configured pipelines for the
/// same signal.
@NullMarked
package dev.nthings.otlp4j.core.pipeline;

import org.jspecify.annotations.NullMarked;
