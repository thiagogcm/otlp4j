package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import java.util.ArrayList;
import java.util.List;

/// Maps OTLP logs types between proto and domain, in both directions.
///
/// Internal. Part of the transport layer; not public API.
final class LogsMapper {

    private LogsMapper() {}

    // --- proto item count ---

    /// Counts log records in a proto logs export request without decoding it to the domain, walking
    /// the full resource then scope then record hierarchy so the count agrees with
    /// `LogsData.logRecordCount`.
    ///
    /// @param request the proto logs export request
    /// @return the total log record count
    static long itemCount(ExportLogsServiceRequest request) {
        long count = 0;
        for (var rl : request.getResourceLogsList()) {
            for (var sl : rl.getScopeLogsList()) {
                count += sl.getLogRecordsCount();
            }
        }
        return count;
    }

    /// The resource-level attributes of a proto logs export request, one entry per resource in wire
    /// order. Converts only the top-level resource attribute lists through [CommonMapper#attributes],
    /// so the log records are never decoded and no domain batch is materialised.
    ///
    /// @param request the proto logs export request
    /// @return the per-resource attributes in wire order
    static List<Attributes> protoResourceAttributes(ExportLogsServiceRequest request) {
        var result = new ArrayList<Attributes>(request.getResourceLogsCount());
        for (var rl : request.getResourceLogsList()) {
            result.add(CommonMapper.attributes(rl.getResource().getAttributesList()));
        }
        return result;
    }

    // --- proto -> domain ---

    static LogsData toDomain(ExportLogsServiceRequest request) {
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

    /// Interprets an OTLP logs export response as a [dev.nthings.otlp4j.core.result.DeliveryResult].
    ///
    /// @param response   the logs export response
    /// @param provenance the provenance to attribute the result to
    /// @return the delivery result
    static dev.nthings.otlp4j.core.result.DeliveryResult deliveryResult(
            ExportLogsServiceResponse response, dev.nthings.otlp4j.core.result.Provenance provenance) {
        var partial = response.getPartialSuccess();
        return CommonMapper.deliveryResult(
                response.hasPartialSuccess(), partial.getRejectedLogRecords(), partial.getErrorMessage(), provenance);
    }

    // --- domain -> proto ---

    static ExportLogsServiceRequest toProto(LogsData logs) {
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
