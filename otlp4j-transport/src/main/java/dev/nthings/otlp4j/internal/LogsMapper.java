package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.pipeline.ConsumeResult;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import java.util.ArrayList;
import java.util.List;

/// Maps OTLP logs types between the generated proto layer and the `dev.nthings.otlp4j.model`
/// domain types, in both directions.
///
/// **Internal.** Part of the transport layer; not public API.
final class LogsMapper {

    private LogsMapper() {}

    // --- proto -> domain ---------------------------------------------------------------------

    public static LogsData toDomain(ExportLogsServiceRequest request) {
        var resourceLogs = new ArrayList<LogsData.ResourceLogs>(request.getResourceLogsCount());
        for (var rl : request.getResourceLogsList()) {
            resourceLogs.add(toDomain(rl));
        }
        return new LogsData(resourceLogs);
    }

    private static LogsData.ResourceLogs toDomain(io.opentelemetry.proto.logs.v1.ResourceLogs rl) {
        var scopeLogs = new ArrayList<LogsData.ScopeLogs>(rl.getScopeLogsCount());
        for (var sl : rl.getScopeLogsList()) {
            scopeLogs.add(toDomain(sl));
        }
        return new LogsData.ResourceLogs(
                CommonMapper.resource(rl.getResource()), rl.getSchemaUrl(), scopeLogs);
    }

    private static LogsData.ScopeLogs toDomain(io.opentelemetry.proto.logs.v1.ScopeLogs sl) {
        var logRecords = new ArrayList<LogRecord>(sl.getLogRecordsCount());
        for (var record : sl.getLogRecordsList()) {
            logRecords.add(toDomain(record));
        }
        return new LogsData.ScopeLogs(
                CommonMapper.scope(sl.getScope()), sl.getSchemaUrl(), logRecords);
    }

    private static LogRecord toDomain(io.opentelemetry.proto.logs.v1.LogRecord record) {
        return new LogRecord(
                record.getTimeUnixNano(),
                record.getObservedTimeUnixNano(),
                LogRecord.Severity.fromNumber(record.getSeverityNumber().getNumber()),
                record.getSeverityText(),
                CommonMapper.attributeValue(record.getBody()),
                CommonMapper.attributes(record.getAttributesList()),
                record.getDroppedAttributesCount(),
                record.getFlags() & 0xFFFFFFFFL,
                CommonMapper.hex(record.getTraceId()),
                CommonMapper.hex(record.getSpanId()),
                record.getEventName());
    }

    /// Interprets an OTLP logs export response as a [ConsumeResult].
    public static ConsumeResult<LogsData> result(ExportLogsServiceResponse response) {
        if (!response.hasPartialSuccess()) {
            return ConsumeResult.accepted();
        }
        var partial = response.getPartialSuccess();
        if (partial.getRejectedLogRecords() == 0 && partial.getErrorMessage().isEmpty()) {
            return ConsumeResult.accepted();
        }
        if (partial.getRejectedLogRecords() == 0) {
            return ConsumeResult.rejected(partial.getErrorMessage());
        }
        return ConsumeResult.partial(partial.getRejectedLogRecords(), partial.getErrorMessage());
    }

    // --- domain -> proto ---------------------------------------------------------------------

    public static ExportLogsServiceRequest toProto(LogsData logs) {
        var request = ExportLogsServiceRequest.newBuilder();
        for (var rl : logs.resourceLogs()) {
            request.addResourceLogs(toProto(rl));
        }
        return request.build();
    }

    private static io.opentelemetry.proto.logs.v1.ResourceLogs toProto(LogsData.ResourceLogs rl) {
        var builder =
                io.opentelemetry.proto.logs.v1.ResourceLogs.newBuilder()
                        .setResource(CommonMapper.toProtoResource(rl.resource()))
                        .setSchemaUrl(rl.schemaUrl());
        for (var sl : rl.scopeLogs()) {
            builder.addScopeLogs(toProto(sl));
        }
        return builder.build();
    }

    private static io.opentelemetry.proto.logs.v1.ScopeLogs toProto(LogsData.ScopeLogs sl) {
        var builder =
                io.opentelemetry.proto.logs.v1.ScopeLogs.newBuilder()
                        .setScope(CommonMapper.toProtoScope(sl.scope()))
                        .setSchemaUrl(sl.schemaUrl());
        for (var record : sl.logRecords()) {
            builder.addLogRecords(toProto(record));
        }
        return builder.build();
    }

    private static io.opentelemetry.proto.logs.v1.LogRecord toProto(LogRecord record) {
        return io.opentelemetry.proto.logs.v1.LogRecord.newBuilder()
                .setTimeUnixNano(record.epochNanos())
                .setObservedTimeUnixNano(record.observedEpochNanos())
                .setSeverityNumber(SeverityNumber.forNumber(record.severity().number()))
                .setSeverityText(record.severityText())
                .setBody(CommonMapper.toAnyValue(record.body()))
                .addAllAttributes(CommonMapper.toKeyValues(record.attributes()))
                .setDroppedAttributesCount(record.droppedAttributesCount())
                .setFlags((int) record.flags())
                .setTraceId(CommonMapper.bytes(record.traceId()))
                .setSpanId(CommonMapper.bytes(record.spanId()))
                .setEventName(record.eventName())
                .build();
    }
}
