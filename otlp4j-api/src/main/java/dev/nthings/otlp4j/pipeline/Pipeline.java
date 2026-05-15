package dev.nthings.otlp4j.pipeline;

import java.util.ArrayList;
import java.util.List;

/// Composes [Processor]s in front of a terminal [TelemetryConsumer].
///
/// Processors run in the order they are added, then telemetry flows into the terminal consumer.
public final class Pipeline {

    private Pipeline() {}

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private final List<Processor> processors = new ArrayList<>();

        private Builder() {}

        /// Appends a processing stage; stages run in the order they are added.
        public Builder process(Processor processor) {
            processors.add(processor);
            return this;
        }

        /// Closes the pipeline with a terminal consumer, returning the fully composed consumer.
        public TelemetryConsumer into(TelemetryConsumer terminal) {
            var consumer = terminal;
            for (var i = processors.size() - 1; i >= 0; i--) {
                consumer = processors.get(i).apply(consumer);
            }
            return consumer;
        }
    }
}
