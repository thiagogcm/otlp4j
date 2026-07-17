package dev.nthings.otlp4j.transport.otlp;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.transport.common.ExporterComponentAdapter;
import dev.nthings.otlp4j.transport.common.OtlpExporterConfig;

/// The graph [ExporterFactory] for the Armeria OTLP/HTTP exporter. It creates one exporter bound to a
/// signal by wiring the [OtlpArmeriaExporter] HTTP builder through an [ExporterComponentAdapter], so
/// one shared client serves every signal that terminates at one endpoint. Mirror of the legacy
/// OTLP/HTTP exporter factory.
public final class OtlpArmeriaHttpExporterFactory implements ExporterFactory<OtlpExporterConfig> {

    /// The conventional OTLP/HTTP port (4318).
    public static final int DEFAULT_PORT = 4318;

    /// The component type name for the OTLP/HTTP exporter.
    public static final String TYPE = "otlphttp";

    @Override
    public ComponentKind kind() {
        return ComponentKind.EXPORTER;
    }

    @Override
    public OtlpExporterConfig defaultConfig() {
        return OtlpExporterConfig.defaults(DEFAULT_PORT);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public <T extends TelemetryPayload> Exporter<T> create(
            CreateContext ctx, OtlpExporterConfig config, SignalDescriptor<T> signal) {
        return ExporterComponentAdapter.create(
                OtlpArmeriaExporter.httpBuilder(), config, ctx.id(), signal, ctx.base(), ctx.sharedComponents());
    }
}
