package dev.nthings.otlp4j.opentelemetry;

import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.model.ProfilesData;
import io.opentelemetry.sdk.profiles.ProfileExporter;
import java.util.Objects;

/**
 * The factory method for adapting an in-process otlp4j input to an OpenTelemetry SDK profile exporter.
 *
 * <p>Experimental. It is apart from {@link SdkExporters} because it needs the SDK's alpha profiles
 * artifact, which the bridge leaves optional: the JVM verifying a class that named its types would
 * fail for an application using only the stable signals.
 */
public final class SdkProfileExporters {

    private SdkProfileExporters() {}

    /**
     * Creates a profile exporter that submits SDK snapshots to {@code input}.
     *
     * <p>The returned exporter borrows the input, on the terms {@link SdkExporters} describes.
     *
     * @param input the profiles input to borrow
     * @return an OpenTelemetry SDK profile exporter
     */
    public static ProfileExporter profiles(TelemetryInput<ProfilesData> input) {
        return new SdkProfileExporter(Objects.requireNonNull(input, "input"));
    }
}
