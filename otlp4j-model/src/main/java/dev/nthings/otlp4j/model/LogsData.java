package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/// A batch of log telemetry: the domain equivalent of an OTLP `ExportLogsServiceRequest`.
///
/// Hierarchy: `LogsData → ResourceLogs → ScopeLogs → LogRecord`.
public record LogsData(List<ResourceLogs> resourceLogs) {

    public LogsData {
        resourceLogs = List.copyOf(resourceLogs);
    }

    /// Wraps `logRecords` under one `resource` and `scope`.
    public static LogsData of(Resource resource, InstrumentationScope scope, List<LogRecord> logRecords) {
        return new LogsData(List.of(new ResourceLogs(resource, "", List.of(new ScopeLogs(scope, "", logRecords)))));
    }

    /// All log records across every resource and scope, flattened for convenient consumption.
    ///
    /// This walks the resource/scope grouping and allocates a fresh list on every call. On a hot
    /// path prefer [#forEachLogRecord] to visit records without the intermediate list, or
    /// [#logRecordCount] to size the batch without flattening it.
    public List<LogRecord> logRecords() {
        return resourceLogs.stream()
                .flatMap(rl -> rl.scopeLogs().stream())
                .flatMap(sl -> sl.logRecords().stream())
                .toList();
    }

    /// Applies `action` to every log record across every resource and scope, in the same order as
    /// [#logRecords], without allocating the intermediate flattened list.
    public void forEachLogRecord(Consumer<? super LogRecord> action) {
        Objects.requireNonNull(action, "action");
        for (var resource : resourceLogs) {
            for (var scope : resource.scopeLogs()) {
                for (var logRecord : scope.logRecords()) {
                    action.accept(logRecord);
                }
            }
        }
    }

    /// The total number of log records across every resource and scope, counted without allocating
    /// the flattened list that [#logRecords] builds. This is the OTLP item count used for batching
    /// and partial-success accounting.
    public int logRecordCount() {
        var count = 0;
        for (var resource : resourceLogs) {
            for (var scope : resource.scopeLogs()) {
                count += scope.logRecords().size();
            }
        }
        return count;
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
