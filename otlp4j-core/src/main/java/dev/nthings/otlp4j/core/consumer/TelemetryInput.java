package dev.nthings.otlp4j.core.consumer;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * A typed in-process ingress for one telemetry signal.
 *
 * <p>The input itself has no implicit queue. A submission acknowledges the next configured graph
 * boundary: with a direct path its stage can represent the downstream outcome, while a buffering
 * component may complete it as soon as that component accepts ownership. Failures after buffered
 * acceptance are surfaced by {@link #flush(Duration)}.
 *
 * @param <T> the signal's domain batch type
 */
public interface TelemetryInput<T> {

    /** @return the signal accepted by this input */
    SignalDescriptor<T> signal();

    /** Submits a batch with {@link ConsumeContext#defaults()}. */
    default CompletionStage<DeliveryResult> submit(T data) {
        return submit(data, ConsumeContext.defaults());
    }

    /**
     * Submits a batch and completes with the result from the next graph boundary.
     *
     * @param data the domain batch
     * @param context the context propagated to graph consumers
     * @return a stage completing with the admission result
     */
    CompletionStage<DeliveryResult> submit(T data, ConsumeContext context);

    /**
     * Flushes accepted work and reports asynchronous downstream outcomes observed while draining.
     *
     * @param timeout maximum time to drain
     * @return a stage completing with the flush report
     */
    CompletionStage<FlushReport> flush(Duration timeout);
}
