package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.util.concurrent.CompletionStage;

/// Wraps the exporter send boundary with the two counters the transport exporter feeds today:
/// `otlp4j.exporter.export.success` and `otlp4j.exporter.export.failure`. The report places this
/// exporter metrics helper in the service module while retry and timeout stay in the transport, and
/// keeps it thin in the first milestone: it observes the outcome of a send and increments the success
/// counter for an accepted or partial result and the failure counter for a rejection, leaving the
/// send itself untouched.
public final class ExporterMetricsHelper {

    /// The instrument name for successful exporter sends.
    public static final String SUCCESS_COUNTER = "otlp4j.exporter.export.success";

    /// The instrument name for failed exporter sends.
    public static final String FAILURE_COUNTER = "otlp4j.exporter.export.failure";

    private final GatewayMetrics.LongCounter success;

    private final GatewayMetrics.LongCounter failure;

    /// Builds the helper from a metrics registry, registering the two counters.
    ///
    /// @param metrics the registry to register the counters with
    public ExporterMetricsHelper(GatewayMetrics metrics) {
        if (metrics == null) {
            throw new IllegalArgumentException("metrics must be non-null; use GatewayMetrics.noop()");
        }
        this.success = metrics.counter(SUCCESS_COUNTER, "exporter sends that were accepted downstream");
        this.failure = metrics.counter(FAILURE_COUNTER, "exporter sends that were rejected downstream");
    }

    /// Observes the outcome of a send: increments the success counter when the send completes with an
    /// accepted or partial result and the failure counter when it completes with a rejection or fails
    /// exceptionally, then passes the result through unchanged.
    ///
    /// @param send the send stage to observe
    /// @return the same stage, with the outcome counted
    public CompletionStage<DeliveryResult> observe(CompletionStage<DeliveryResult> send) {
        return send.whenComplete((result, failureCause) -> {
            if (failureCause != null || result == null) {
                failure.add(1);
                return;
            }
            switch (result) {
                case DeliveryResult.Accepted ignored -> success.add(1);
                case DeliveryResult.Partial ignored -> success.add(1);
                case DeliveryResult.Rejected ignored -> failure.add(1);
            }
        });
    }
}
