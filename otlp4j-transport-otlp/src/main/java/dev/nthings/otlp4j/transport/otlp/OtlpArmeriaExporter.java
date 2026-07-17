package dev.nthings.otlp4j.transport.otlp;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.transport.common.AbstractExporterBuilder;
import dev.nthings.otlp4j.transport.common.ClientExporter;
import dev.nthings.otlp4j.transport.common.ExporterMetrics;
import dev.nthings.otlp4j.transport.otlp.internal.ArmeriaOtlpClient;
import dev.nthings.otlp4j.transport.otlp.internal.Protocol;

/// Builds [ClientExporter]s that send typed telemetry to an OTLP endpoint over Armeria.
///
/// One exporter handles all four signals via per-signal facets; flush/drain/shutdown live on the
/// returned [ClientExporter]. [#grpcBuilder()] speaks OTLP/gRPC (default `localhost:4317`),
/// [#httpBuilder()] speaks OTLP/HTTP (default `localhost:4318`, POSTing each signal to its standard
/// `/v1/...` path). The scheme is chosen by the configured TLS mode.
public final class OtlpArmeriaExporter {

    private OtlpArmeriaExporter() {}

    /// A builder speaking OTLP/gRPC, default endpoint `localhost:4317`.
    ///
    /// @return the gRPC exporter builder
    public static Builder grpcBuilder() {
        return new Builder(Protocol.GRPC, ClientConfig.builder(), OtlpArmeriaExporter.class.getName() + "/gRPC");
    }

    /// A builder speaking OTLP/HTTP, default endpoint `localhost:4318`.
    ///
    /// @return the HTTP exporter builder
    public static Builder httpBuilder() {
        return new Builder(Protocol.HTTP, ClientConfig.builder().setPort(4318),
                OtlpArmeriaExporter.class.getName() + "/HTTP");
    }

    /// Builder for an Armeria OTLP exporter; the shared knobs live on [AbstractExporterBuilder]. The
    /// path prefix and connect timeout are honoured by OTLP/HTTP and ignored by OTLP/gRPC, matching
    /// the neutral config semantics.
    public static final class Builder extends AbstractExporterBuilder<Builder> {

        private final Protocol protocol;

        private Builder(Protocol protocol, ClientConfig.Builder config, String exporterType) {
            super(config, exporterType);
            this.protocol = protocol;
        }

        /// Sets an endpoint path prefix (e.g. `/otlp`) prepended to the per-signal paths; blank or `/`
        /// mean none. Honoured by OTLP/HTTP, ignored by OTLP/gRPC.
        ///
        /// @param path the endpoint path prefix
        /// @return this builder
        public Builder setPath(String path) {
            config().setPath(path);
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected OtlpClient newClient(ClientConfig config, ExporterMetrics metrics) {
            return new ArmeriaOtlpClient(config, metrics, protocol);
        }
    }
}
