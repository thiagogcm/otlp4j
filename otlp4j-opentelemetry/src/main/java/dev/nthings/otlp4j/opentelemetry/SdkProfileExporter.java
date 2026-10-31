package dev.nthings.otlp4j.opentelemetry;

import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.model.ProfilesData;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.profiles.ProfileExporter;
import io.opentelemetry.sdk.profiles.data.ProfileData;
import java.util.Collection;
import java.util.Objects;

final class SdkProfileExporter extends ExportLifecycle<ProfilesData> implements ProfileExporter {

    SdkProfileExporter(TelemetryInput<ProfilesData> input) {
        super(input);
    }

    @Override
    public CompletableResultCode export(Collection<ProfileData> profiles) {
        Objects.requireNonNull(profiles, "profiles");
        return exportSnapshot(() -> SdkProfileResolver.batch(ListCopy.copy(profiles)));
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
