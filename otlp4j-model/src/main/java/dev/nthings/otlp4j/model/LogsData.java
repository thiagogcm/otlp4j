package dev.nthings.otlp4j.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

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
    /// Allocates a fresh list on every call; on a hot path prefer [#forEachLogRecord] or
    /// [#logRecordCount].
    public List<LogRecord> logRecords() {
        return resourceLogs.stream()
                .flatMap(rl -> rl.scopeLogs().stream())
                .flatMap(sl -> sl.logRecords().stream())
                .toList();
    }

    /// Applies `action` to every log record across every resource and scope, in [#logRecords] order
    /// without allocating the flattened list.
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
    /// the list [#logRecords] builds.
    public int logRecordCount() {
        var count = 0;
        for (var resource : resourceLogs) {
            for (var scope : resource.scopeLogs()) {
                count += scope.logRecords().size();
            }
        }
        return count;
    }

    /// Returns this batch with every resource replaced by `mapper`'s result. Schema URLs, scopes, and
    /// log records are retained. Returns this instance when `mapper` returns each resource unchanged.
    public LogsData mapResources(UnaryOperator<Resource> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return withResourceLogs(Rewrites.map(resourceLogs, group -> group.withResource(
                Objects.requireNonNull(mapper.apply(group.resource()), "mapper returned null"))));
    }

    /// Returns this batch with only the resource groups whose resource `keep` accepts. A rejected
    /// group is removed together with its log records. Returns this instance when `keep` accepts every
    /// resource.
    public LogsData filterResources(Predicate<? super Resource> keep) {
        Objects.requireNonNull(keep, "keep");
        return withResourceLogs(Rewrites.filter(resourceLogs, group -> keep.test(group.resource())));
    }

    /// Returns this batch with every log record replaced by `mapper`'s result, which must be non-null.
    /// Resource and scope grouping is retained. Returns this instance when `mapper` returns each
    /// log record unchanged.
    public LogsData mapLogRecords(UnaryOperator<LogRecord> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return withResourceLogs(Rewrites.map(resourceLogs, resource -> resource.withScopeLogs(
                Rewrites.map(resource.scopeLogs(), scope -> scope.withLogRecords(
                        Rewrites.map(scope.logRecords(), mapper))))));
    }

    /// Returns this batch with only the log records `keep` accepts, in their original order and
    /// grouping. A scope or resource that loses all its log records is removed; groups that lose nothing
    /// are kept as they are. Returns this instance when `keep` accepts every log record.
    public LogsData filterLogRecords(Predicate<? super LogRecord> keep) {
        Objects.requireNonNull(keep, "keep");
        return withResourceLogs(Rewrites.prune(
                resourceLogs,
                resource -> resource.withScopeLogs(Rewrites.prune(
                        resource.scopeLogs(),
                        scope -> scope.withLogRecords(Rewrites.filter(scope.logRecords(), keep)),
                        scope -> scope.logRecords().isEmpty())),
                resource -> resource.scopeLogs().isEmpty()));
    }

    private LogsData withResourceLogs(List<ResourceLogs> replacement) {
        return replacement == resourceLogs ? this : new LogsData(replacement);
    }

    /// Log records from one [Resource], grouped by instrumentation scope.
    public record ResourceLogs(Resource resource, String schemaUrl, List<ScopeLogs> scopeLogs) {
        public ResourceLogs {
            scopeLogs = List.copyOf(scopeLogs);
        }

        ResourceLogs withResource(Resource replacement) {
            return replacement == resource ? this : new ResourceLogs(replacement, schemaUrl, scopeLogs);
        }

        ResourceLogs withScopeLogs(List<ScopeLogs> replacement) {
            return replacement == scopeLogs ? this : new ResourceLogs(resource, schemaUrl, replacement);
        }
    }

    /// Log records produced by a single [InstrumentationScope].
    public record ScopeLogs(
            InstrumentationScope scope, String schemaUrl, List<LogRecord> logRecords) {
        public ScopeLogs {
            logRecords = List.copyOf(logRecords);
        }

        ScopeLogs withLogRecords(List<LogRecord> replacement) {
            return replacement == logRecords ? this : new ScopeLogs(scope, schemaUrl, replacement);
        }
    }
}
