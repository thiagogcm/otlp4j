package dev.nthings.otlp4j.transport.http;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.pipeline.OtlpExporter;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.transport.common.AbstractExporterBuilder;
import dev.nthings.otlp4j.transport.http.internal.HttpOtlpClient;
import java.time.Duration;

/// Builds [OtlpExporter]s that send typed telemetry to an OTLP/HTTP endpoint.
///
/// One exporter handles all four signals via per-signal facets; flush/drain/shutdown live on the
/// returned [OtlpExporter]. Each signal is POSTed to its standard path
/// (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`) under the configured
/// `host:port`, with the scheme chosen by [Tls]. The default endpoint is `localhost:4318`.
public final class OtlpHttpExporter {

    /// The conventional OTLP/HTTP port (4318); gRPC uses 4317.
    static final int DEFAULT_HTTP_PORT = 4318;

    private OtlpHttpExporter() {}

    public static Builder builder() {
        return new Builder();
    }

    /// Convenience factory: builds and returns a ready-to-use [OtlpExporter].
    public static OtlpExporter to(String host, int port) {
        return builder().setEndpoint(host, port).build();
    }

    /// Builds from the standard `OTEL_EXPORTER_OTLP_*` variables.
    /// Equivalent to `builder().fromEnvironment().build()`.
    public static OtlpExporter fromEnvironment() {
        return builder().fromEnvironment().build();
    }

    /// Builder for [OtlpHttpExporter]. Defaults to `localhost:4318` with a 10s deadline; the
    /// shared knobs live on [AbstractExporterBuilder].
    public static final class Builder extends AbstractExporterBuilder<Builder> {

        private Builder() {
            super(ClientConfig.builder().setPort(DEFAULT_HTTP_PORT), OtlpHttpExporter.class.getName());
        }

        /// Sets an endpoint path prefix (e.g. `/otlp`) prepended to the per-signal paths; blank or
        /// `/` mean none.
        ///
        /// @param path the endpoint path prefix
        /// @return this builder
        public Builder setPath(String path) {
            config().setPath(path);
            return this;
        }

        /// Bounds connection setup; unset falls back to [#setTimeout(Duration)].
        ///
        /// @param connectTimeout the connection-setup deadline
        /// @return this builder
        public Builder setConnectTimeout(Duration connectTimeout) {
            config().setConnectTimeout(connectTimeout);
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected OtlpClient newClient(ClientConfig config) {
            return new HttpOtlpClient(config);
        }
    }
}
