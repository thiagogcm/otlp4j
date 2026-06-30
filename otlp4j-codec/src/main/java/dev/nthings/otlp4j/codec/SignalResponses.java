package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesPartialSuccess;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;

/// Builds the four OTLP `Export*ServiceResponse` messages from a [ConsumeResult], shared
/// by gRPC and HTTP adapters so both encode partial success identically.
///
/// [ConsumeResult.Accepted] leaves `partial_success` unset; [ConsumeResult.Partial] carries
/// the rejected count and message. [ConsumeResult.Rejected] is never a response message -
/// both transports intercept it first (gRPC error / HTTP 5xx) - so its switch arm only
/// keeps exhaustiveness and throws [#rejectedNotMapped()] if ever reached.
public final class SignalResponses {

    private SignalResponses() {}

    public static ExportTraceServiceResponse traces(ConsumeResult<TracesData> result) {
        var resp = ExportTraceServiceResponse.newBuilder();
        switch (result) {
            case ConsumeResult.Accepted<TracesData> _ -> { }
            case ConsumeResult.Partial<TracesData>(var rejected, var message) ->
                    resp.setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                            .setRejectedSpans(rejected)
                            .setErrorMessage(message));
            case ConsumeResult.Rejected<TracesData> _ -> throw rejectedNotMapped();
        }
        return resp.build();
    }

    public static ExportMetricsServiceResponse metrics(ConsumeResult<MetricsData> result) {
        var resp = ExportMetricsServiceResponse.newBuilder();
        switch (result) {
            case ConsumeResult.Accepted<MetricsData> _ -> { }
            case ConsumeResult.Partial<MetricsData>(var rejected, var message) ->
                    resp.setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                            .setRejectedDataPoints(rejected)
                            .setErrorMessage(message));
            case ConsumeResult.Rejected<MetricsData> _ -> throw rejectedNotMapped();
        }
        return resp.build();
    }

    public static ExportLogsServiceResponse logs(ConsumeResult<LogsData> result) {
        var resp = ExportLogsServiceResponse.newBuilder();
        switch (result) {
            case ConsumeResult.Accepted<LogsData> _ -> { }
            case ConsumeResult.Partial<LogsData>(var rejected, var message) ->
                    resp.setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                            .setRejectedLogRecords(rejected)
                            .setErrorMessage(message));
            case ConsumeResult.Rejected<LogsData> _ -> throw rejectedNotMapped();
        }
        return resp.build();
    }

    public static ExportProfilesServiceResponse profiles(ConsumeResult<ProfilesData> result) {
        var resp = ExportProfilesServiceResponse.newBuilder();
        switch (result) {
            case ConsumeResult.Accepted<ProfilesData> _ -> { }
            case ConsumeResult.Partial<ProfilesData>(var rejected, var message) ->
                    resp.setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                            .setRejectedProfiles(rejected)
                            .setErrorMessage(message));
            case ConsumeResult.Rejected<ProfilesData> _ -> throw rejectedNotMapped();
        }
        return resp.build();
    }

    /// Guards the unreachable [ConsumeResult.Rejected] switch arms. Callers map a whole-batch
    /// rejection to a transport error before building a response message.
    static IllegalStateException rejectedNotMapped() {
        return new IllegalStateException(
                "ConsumeResult.Rejected is mapped to a transport error, not to a response message");
    }
}
