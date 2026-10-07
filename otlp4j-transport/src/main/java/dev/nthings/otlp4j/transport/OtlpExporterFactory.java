package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.transport.internal.Protocol;

/// The graph [ExporterFactory] for the OTLP exporter, protocol-parameterized through [#grpc()] and
/// [#http()]. Each factory creates one exporter bound to a signal by building a shared
/// [OtlpExporter] (once per component id, through the build-scoped [CreateContext#sharedComponents])
/// and handing each per-signal pipeline the matching facet, so one shared channel serves every signal
/// that terminates at one endpoint. The protocol derives the component [#type()] (`otlp` for gRPC,
/// `otlphttp` for HTTP) and the [#defaultPort()] (`4317` / `4318`).
public final class OtlpExporterFactory implements ExporterFactory<OtlpExporterConfig> {

    private final Protocol protocol;
    private final String type;
    private final int defaultPort;

    private OtlpExporterFactory(Protocol protocol, String type, int defaultPort) {
        this.protocol = protocol;
        this.type = type;
        this.defaultPort = defaultPort;
    }

    /// The OTLP/gRPC exporter factory (`otlp`, default port `4317`).
    ///
    /// @return the gRPC exporter factory
    public static OtlpExporterFactory grpc() {
        return new OtlpExporterFactory(Protocol.GRPC, "otlp", Ports.GRPC);
    }

    /// The OTLP/HTTP exporter factory (`otlphttp`, default port `4318`).
    ///
    /// @return the HTTP exporter factory
    public static OtlpExporterFactory http() {
        return new OtlpExporterFactory(Protocol.HTTP, "otlphttp", Ports.HTTP);
    }

    /// The conventional port for this factory's protocol (`4317` for gRPC, `4318` for HTTP).
    ///
    /// @return the default endpoint port
    public int defaultPort() {
        return defaultPort;
    }

    @Override
    public ComponentKind kind() {
        return ComponentKind.EXPORTER;
    }

    @Override
    public OtlpExporterConfig defaultConfig() {
        return OtlpExporterConfig.defaults(defaultPort);
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public <T> Exporter<T> create(
            CreateContext ctx, OtlpExporterConfig config, SignalDescriptor<T> signal) {
        // The first facet of an id builds the shared OtlpExporter through the build-scoped cache;
        // later facets of the same id reuse it, so a single exporter id used by several pipelines opens
        // exactly one wire channel. Each facet retains a reference-counted stake, so the channel closes
        // only when the last facet shuts down.
        OtlpExporter exporter = ctx.sharedComponents().computeIfAbsent(ctx.id(), () ->
                OtlpExporter.forFactory(protocol, config, ctx.id(), ctx.base().metrics()));
        return exporter.facet(signal);
    }
}
