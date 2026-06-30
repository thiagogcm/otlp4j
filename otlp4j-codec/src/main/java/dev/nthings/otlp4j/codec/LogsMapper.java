package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.ConsumeResult;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import java.util.ArrayList;

/// Maps OTLP logs types between proto and domain, in both directions.
///
/// Internal. Part of the transport layer; not public API.
public final class LogsMapper {

    private LogsMapper() {}

    // --- proto -> domain ---

    public static LogsData toDomain(ExportLogsServiceRequest request) {
        var resourceLogs = new ArrayList<LogsData.ResourceLogs>(request.getResourceLogsCount());
        for (var rl : request.getResourceLogsList()) {
            resourceLogs.add(toDomain(rl));
        }
        return new LogsData(resourceLogs);
    }

    private static LogsData.ResourceLogs toDomain(ResourceLogs rl) {
        var scopeLogs = new ArrayList<LogsData.ScopeLogs>(rl.getScopeLogsCount());
        for (var sl : rl.getScopeLogsList()) {
            scopeLogs.add(toDomain(sl));
        }
        return new LogsData.ResourceLogs(
                CommonMapper.resource(rl.getResource()), rl.getSchemaUrl(), scopeLogs);
    }

    private static LogsData.ScopeLogs toDomain(ScopeLogs sl) {
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
                LogRecord.Severity.fromNumber(record.getSeverityNumberValue()),
                record.getSeverityText(),
                CommonMapper.attributeValue(record.getBody()),
                CommonMapper.attributes(record.getAttributesList()),
                record.getDroppedAttributesCount(),
                Integer.toUnsignedLong(record.getFlags()),
                CommonMapper.hex(record.getTraceId()),
                CommonMapper.hex(record.getSpanId()),
                record.getEventName());
    }

    /// Interprets an OTLP logs export response as a [ConsumeResult].
    public static ConsumeResult<LogsData> result(ExportLogsServiceResponse response) {
        var partial = response.getPartialSuccess();
        return CommonMapper.result(
                response.hasPartialSuccess(), partial.getRejectedLogRecords(), partial.getErrorMessage());
    }

    // --- domain -> proto ---

    public static ExportLogsServiceRequest toProto(LogsData logs) {
        var request = ExportLogsServiceRequest.newBuilder();
        for (var rl : logs.resourceLogs()) {
            request.addResourceLogs(toProto(rl));
        }
        return request.build();
    }

    private static ResourceLogs toProto(LogsData.ResourceLogs rl) {
        var builder =
                ResourceLogs.newBuilder()
                        .setResource(CommonMapper.toProtoResource(rl.resource()))
                        .setSchemaUrl(rl.schemaUrl());
        for (var sl : rl.scopeLogs()) {
            builder.addScopeLogs(toProto(sl));
        }
        return builder.build();
    }

    private static ScopeLogs toProto(LogsData.ScopeLogs sl) {
        var builder =
                ScopeLogs.newBuilder()
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
