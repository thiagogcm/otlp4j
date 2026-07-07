package dev.nthings.otlp4j.transport.grpc;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.transport.common.AbstractExporterBuilder;
import dev.nthings.otlp4j.transport.common.ClientExporter;
import dev.nthings.otlp4j.transport.common.ExporterMetrics;
import dev.nthings.otlp4j.transport.grpc.internal.GrpcOtlpClient;

/// Builds [ClientExporter]s that send typed telemetry to an OTLP/gRPC endpoint.
///
/// One exporter handles all four signals via per-signal facets; flush/drain/shutdown live on the
/// returned [ClientExporter]. The default endpoint is `localhost:4317`.
public final class OtlpGrpcExporter {

    private OtlpGrpcExporter() {}

    public static Builder builder() {
        return new Builder();
    }

    /// Convenience factory: builds and returns a ready-to-use [ClientExporter].
    public static ClientExporter to(String host, int port) {
        return builder().setEndpoint(host, port).build();
    }

    /// Builds from the standard `OTEL_EXPORTER_OTLP_*` variables (see [Builder#fromEnvironment()]).
    /// Equivalent to `builder().fromEnvironment().build()`.
    public static ClientExporter fromEnvironment() {
        return builder().fromEnvironment().build();
    }

    /// Builder for [OtlpGrpcExporter]. Defaults to `localhost:4317` with a 10s deadline; the
    /// shared knobs live on [AbstractExporterBuilder].
    public static final class Builder extends AbstractExporterBuilder<Builder> {

        private Builder() {
            super(ClientConfig.builder(), OtlpGrpcExporter.class.getName());
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected OtlpClient newClient(ClientConfig config, ExporterMetrics metrics) {
            return new GrpcOtlpClient(config, metrics);
        }
    }
}
