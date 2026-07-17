package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesPartialSuccess;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;

/// Builds the four OTLP `Export*ServiceResponse` messages from a [DeliveryResult], shared by gRPC
/// and HTTP adapters so both encode partial success identically.
///
/// [DeliveryResult.Accepted] leaves `partial_success` unset; [DeliveryResult.Partial] carries the
/// rejected count and message. [DeliveryResult.Rejected] is never a response message: both transports
/// intercept it first (gRPC error / HTTP 5xx), so its switch arm only keeps exhaustiveness and throws
/// [#rejectedNotMapped()] if ever reached.
final class DeliveryResultResponses {

    private DeliveryResultResponses() {}

    /// Builds the trace export response from a delivery result.
    ///
    /// @param result the delivery result
    /// @return the trace service response
    static ExportTraceServiceResponse traces(DeliveryResult result) {
        var resp = ExportTraceServiceResponse.newBuilder();
        switch (result) {
            case DeliveryResult.Accepted _ -> { }
            case DeliveryResult.Partial(var rejected, var message, var _) ->
                    resp.setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                            .setRejectedSpans(rejected)
                            .setErrorMessage(message));
            case DeliveryResult.Rejected _ -> throw rejectedNotMapped();
        }
        return resp.build();
    }

    /// Builds the metrics export response from a delivery result.
    ///
    /// @param result the delivery result
    /// @return the metrics service response
    static ExportMetricsServiceResponse metrics(DeliveryResult result) {
        var resp = ExportMetricsServiceResponse.newBuilder();
        switch (result) {
            case DeliveryResult.Accepted _ -> { }
            case DeliveryResult.Partial(var rejected, var message, var _) ->
                    resp.setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                            .setRejectedDataPoints(rejected)
                            .setErrorMessage(message));
            case DeliveryResult.Rejected _ -> throw rejectedNotMapped();
        }
        return resp.build();
    }

    /// Builds the logs export response from a delivery result.
    ///
    /// @param result the delivery result
    /// @return the logs service response
    static ExportLogsServiceResponse logs(DeliveryResult result) {
        var resp = ExportLogsServiceResponse.newBuilder();
        switch (result) {
            case DeliveryResult.Accepted _ -> { }
            case DeliveryResult.Partial(var rejected, var message, var _) ->
                    resp.setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                            .setRejectedLogRecords(rejected)
                            .setErrorMessage(message));
            case DeliveryResult.Rejected _ -> throw rejectedNotMapped();
        }
        return resp.build();
    }

    /// Builds the profiles export response from a delivery result.
    ///
    /// @param result the delivery result
    /// @return the profiles service response
    static ExportProfilesServiceResponse profiles(DeliveryResult result) {
        var resp = ExportProfilesServiceResponse.newBuilder();
        switch (result) {
            case DeliveryResult.Accepted _ -> { }
            case DeliveryResult.Partial(var rejected, var message, var _) ->
                    resp.setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                            .setRejectedProfiles(rejected)
                            .setErrorMessage(message));
            case DeliveryResult.Rejected _ -> throw rejectedNotMapped();
        }
        return resp.build();
    }

    /// Guards the unreachable [DeliveryResult.Rejected] switch arms. Callers map a whole-payload
    /// rejection to a transport error before building a response message.
    ///
    /// @return the exception describing the misuse
    static IllegalStateException rejectedNotMapped() {
        return new IllegalStateException(
                "DeliveryResult.Rejected is mapped to a transport error, not to a response message");
    }
}
