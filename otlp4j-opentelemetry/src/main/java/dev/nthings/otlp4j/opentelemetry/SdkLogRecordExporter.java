package dev.nthings.otlp4j.opentelemetry;

import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.model.LogsData;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;

final class SdkLogRecordExporter extends ExportLifecycle<LogsData> implements LogRecordExporter {

    SdkLogRecordExporter(TelemetryInput<LogsData> input) {
        super(input);
    }

    SdkLogRecordExporter(TelemetryInput<LogsData> input, Duration shutdownTimeout) {
        super(input, shutdownTimeout);
    }

    @Override
    public CompletableResultCode export(Collection<LogRecordData> logs) {
        Objects.requireNonNull(logs, "logs");
        try {
            var snapshot = SdkDataMapper.logs(ListCopy.copy(logs));
            if (snapshot.logRecordCount() == 0) {
                return emptyExport();
            }
            return submit(() -> input().submit(snapshot));
        } catch (RuntimeException exception) {
            return CompletableResultCode.ofExceptionalFailure(exception);
        }
    }

    @Override
    public CompletableResultCode flush() {
        return flushInput();
    }

    @Override
    public CompletableResultCode shutdown() {
        return shutdownAdapter();
    }
}
