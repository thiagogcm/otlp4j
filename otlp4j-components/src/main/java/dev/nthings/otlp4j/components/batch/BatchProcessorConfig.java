package dev.nthings.otlp4j.components.batch;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import java.time.Duration;
import java.util.List;

/// The typed configuration for the batch processor: the neutral core [BatchPolicy] (the flush
/// triggers and metadata keys), the neutral core [DeliveryQueuePolicy] (the bounded queue behind the
/// flush), and the degraded window.
///
/// The degraded window is a components-level field rather than a core policy: it carries the legacy
/// degraded-window duration, over which the delivery queue refuses ingress after a failed downstream
/// delivery, and it is kept off the core policy surface to avoid core churn for a single component's
/// tuning.
///
/// @param batch          the flush-trigger policy: target and maximum sizes, timeout, metadata keys
/// @param delivery       the bounded delivery queue policy behind the flush
/// @param degradedWindow how long the delivery queue refuses ingress after a failed downstream delivery
public record BatchProcessorConfig(BatchPolicy batch, DeliveryQueuePolicy delivery, Duration degradedWindow)
        implements ComponentConfig {

    /// Validates the degraded window, the one field the wrapped core [BatchPolicy] and
    /// [DeliveryQueuePolicy] do not already validate in their own compact constructors.
    public BatchProcessorConfig {
        if (degradedWindow == null || degradedWindow.isNegative()) {
            throw new IllegalArgumentException("degradedWindow must be non-null and non-negative");
        }
    }

    /// The default batch configuration: a one second flush timeout, an 8192-item target with no hard
    /// item or byte cap and no metadata keys, a single-consumer queue of 1024 entries applying
    /// backpressure on overflow without waiting for the downstream result, and a five second degraded
    /// window.
    ///
    /// @return the default configuration
    public static BatchProcessorConfig defaults() {
        return new BatchProcessorConfig(
                new BatchPolicy(8192, 0, 0, Duration.ofSeconds(1), List.of()),
                new DeliveryQueuePolicy(1024, 1, OverflowPolicy.BLOCK, false),
                Duration.ofSeconds(5));
    }
}
