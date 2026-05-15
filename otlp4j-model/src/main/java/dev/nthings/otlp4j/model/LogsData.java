package dev.nthings.otlp4j.model;

import java.util.List;

/// A batch of log telemetry: the domain equivalent of an OTLP `ExportLogsServiceRequest`.
///
/// Hierarchy: `LogsData → ResourceLogs → ScopeLogs → LogRecord`.
public record LogsData(List<ResourceLogs> resourceLogs) {

    public LogsData {
        resourceLogs = List.copyOf(resourceLogs);
    }

    /// All log records across every resource and scope, flattened for convenient consumption.
    public List<LogRecord> logRecords() {
        return resourceLogs.stream()
                .flatMap(rl -> rl.scopeLogs().stream())
                .flatMap(sl -> sl.logRecords().stream())
                .toList();
    }

    /// Log records from one [Resource], grouped by instrumentation scope.
    public record ResourceLogs(Resource resource, String schemaUrl, List<ScopeLogs> scopeLogs) {
        public ResourceLogs {
            scopeLogs = List.copyOf(scopeLogs);
        }
    }

    /// Log records produced by a single [InstrumentationScope].
    public record ScopeLogs(
            InstrumentationScope scope, String schemaUrl, List<LogRecord> logRecords) {
        public ScopeLogs {
            logRecords = List.copyOf(logRecords);
        }
    }
}
