package dev.nthings.otlp4j.transport.http;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.exporter.OtlpExporter;
import dev.nthings.otlp4j.transport.spi.ClientExporter;
import dev.nthings.otlp4j.transport.http.internal.HttpOtlpClient;
import java.time.Duration;
import java.util.Map;

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
        return builder().endpoint(host, port).build();
    }

    /// Builds from the standard `OTEL_EXPORTER_OTLP_*` variables.
    /// Equivalent to `builder().fromEnvironment().build()`.
    public static OtlpExporter fromEnvironment() {
        return builder().fromEnvironment().build();
    }

    /// Builder for [OtlpHttpExporter]. Defaults to `localhost:4318` with a 10s deadline.
    public static final class Builder {

        private ClientConfig.Builder config =
                ClientConfig.builder().port(DEFAULT_HTTP_PORT);

        private Builder() {}

        /// Replaces the whole transport config. The supplied port is used verbatim (the 4318
        /// default applies only to the unconfigured builder).
        public Builder transport(ClientConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        /// Applies the standard `OTEL_EXPORTER_OTLP_*` variables.
        /// Opt-in; call it first so explicit setters override the environment.
        public Builder fromEnvironment() {
            config.fromEnvironment();
            return this;
        }

        public Builder endpoint(String host, int port) {
            config.endpoint(host, port);
            return this;
        }

        /// Sets an endpoint path prefix (e.g. `/otlp`) prepended to the per-signal paths; blank or
        /// `/` mean none.
        public Builder path(String path) {
            config.path(path);
            return this;
        }

        public Builder host(String host) {
            config.host(host);
            return this;
        }

        public Builder port(int port) {
            config.port(port);
            return this;
        }

        public Builder timeout(Duration timeout) {
            config.timeout(timeout);
            return this;
        }

        /// Selects the client TLS mode (e.g. [Tls#systemTrust()] or [Tls.Custom]); also chooses the
        /// `http` vs `https` scheme. Defaults to plaintext `http`.
        public Builder tls(Tls tls) {
            config.tls(tls);
            return this;
        }

        /// Adds one HTTP request header (e.g. `authorization`) sent on every export.
        public Builder header(String key, String value) {
            config.header(key, value);
            return this;
        }

        /// Replaces any existing HTTP request headers with the supplied map.
        public Builder headers(Map<String, String> headers) {
            config.headers(headers);
            return this;
        }

        /// Adds all of `headers` as HTTP request headers, on top of any already set.
        public Builder addHeaders(Map<String, String> headers) {
            config.addHeaders(headers);
            return this;
        }

        /// Selects request-body compression (e.g. [Compression#GZIP] sent as
        /// `Content-Encoding: gzip`). Defaults to none.
        public Builder compression(Compression compression) {
            config.compression(compression);
            return this;
        }

        /// Sets the transport retry policy. Defaults to no retries.
        public Builder retry(RetryPolicy retry) {
            config.retry(retry);
            return this;
        }

        public OtlpExporter build() {
            return new ClientExporter(new HttpOtlpClient(config.build()), OtlpHttpExporter.class.getName());
        }
    }
}
