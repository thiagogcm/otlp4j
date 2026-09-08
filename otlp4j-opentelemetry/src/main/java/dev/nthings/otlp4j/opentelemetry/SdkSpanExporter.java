package dev.nthings.otlp4j.opentelemetry;

import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.model.TracesData;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;

final class SdkSpanExporter extends ExportLifecycle<TracesData> implements SpanExporter {

    SdkSpanExporter(TelemetryInput<TracesData> input) {
        super(input);
    }

    SdkSpanExporter(TelemetryInput<TracesData> input, Duration shutdownTimeout) {
        super(input, shutdownTimeout);
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        Objects.requireNonNull(spans, "spans");
        try {
            var snapshot = SdkDataMapper.traces(ListCopy.copy(spans));
            if (snapshot.spanCount() == 0) {
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
