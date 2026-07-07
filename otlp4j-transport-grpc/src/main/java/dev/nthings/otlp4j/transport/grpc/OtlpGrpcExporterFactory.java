package dev.nthings.otlp4j.transport.grpc;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.transport.common.ExporterComponentAdapter;
import dev.nthings.otlp4j.transport.common.OtlpExporterConfig;

/// The graph [ExporterFactory] for the OTLP/gRPC exporter. It creates one exporter bound to a signal
/// by wiring the existing [OtlpGrpcExporter] builder through an [ExporterComponentAdapter]; the
/// neutral policy fields on [OtlpExporterConfig] are translated to the transport's internal client
/// config, which is the single place the resilience library is still named.
public final class OtlpGrpcExporterFactory implements ExporterFactory<OtlpExporterConfig> {

    /// The conventional OTLP/gRPC port (4317).
    public static final int DEFAULT_PORT = 4317;

    /// The component type name for the OTLP/gRPC exporter.
    public static final String TYPE = "otlp";

    @Override
    public ComponentKind kind() {
        return ComponentKind.EXPORTER;
    }

    @Override
    public StabilityLevel stability() {
        return StabilityLevel.BETA;
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
                OtlpGrpcExporter.builder(), config, ctx.id(), signal, ctx.base(), ctx.sharedComponents());
    }
}
